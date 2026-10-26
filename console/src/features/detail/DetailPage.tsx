import { ArrowLeft, Lock, Pencil, Play, Square, Trash2 } from 'lucide-react';
import { useState, type ReactNode } from 'react';
import { Link, Navigate, useNavigate, useParams, useSearchParams } from 'react-router-dom';

import { EditMaterializationDialog } from './EditMaterializationDialog';
import { DetailOverviewTab } from './OverviewTab';
import { RuntimeTab } from './RuntimeTab';
import { TableTab } from './TableTab';
import { canDelete, canEdit, canResume, canStop, useLifecycleActions } from '../materializations/actions';
import { DataTab } from '../data/DataTab';
import { SchemaTab } from '../schema/SchemaTab';
import { LiveIndicator, PageBody, PageHeader } from '@/components/AppShell';
import { Identifier } from '@/components/Inline';
import { HealthBadge, PhaseBadge } from '@/components/PhaseBadge';
import { Dialog } from '@/components/ui/Dialog';
import { Button, Panel, Skeleton } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { displayName, humanize, topicsLabel } from '@/lib/format';
import { TABLE_PARAM } from '../tables/selectedTable';
import { useAuth } from '@/lib/auth';
import { useDetailStream } from '@/lib/live';
import { readableTables, type CpDetail, type CpTableSpec } from '@shared/types';
import { useDomainPath } from '@/lib/domains';

/**
 * Each tab, and the dataset permission it needs, beyond seeing the materialization at all, on at
 * least one of its tables' datasets: the Schema tab shows the tables whose schemas the person may
 * see, the Data tab those they may query.
 */
const TABS = [
  { key: '', label: 'Overview', permission: null },
  { key: 'schema', label: 'Schema', permission: 'dataset.view_schema' },
  { key: 'data', label: 'Data', permission: 'data.query' },
  { key: 'table', label: 'Table', permission: null },
  { key: 'runtime', label: 'Runtime', permission: null },
] as const;

/** Every table a delete removes - written, kept, or still being dropped - in its dataset. */
function tablesRemoved(detail: CpDetail): CpTableSpec[] {
  const leaving = detail.tables
    .filter((record) => record.state === 'dropping')
    .map((record) => ({ name: record.name, source_topic: record.source_topic, dataset: record.dataset, dataset_id: record.dataset_id }));
  return [...readableTables(detail.materialization), ...leaving];
}

export function DetailPage(): ReactNode {
  const domainPath = useDomainPath();
  const { materializationId, tab } = useParams<{ materializationId: string; tab?: string }>();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { data, status } = useDetailStream(materializationId ?? '');
  const { run, pending } = useLifecycleActions();
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [confirmStop, setConfirmStop] = useState(false);
  const [editing, setEditing] = useState(false);
  const { can } = useAuth();

  if (!materializationId) return <Navigate to={domainPath('/materializations')} replace />;

  const detail = data?.detail ?? null;
  const specs = detail ? readableTables(detail.materialization) : [];
  const tabs = TABS.filter((item) => item.permission === null || specs.some((spec) => can(item.permission, spec.dataset_id)));
  const removed = detail ? tablesRemoved(detail) : [];
  // Deleting takes each table out of its dataset: it needs write access to every one of them.
  const unwritable = [...new Set(removed.filter((spec) => !can('dataset.write', spec.dataset_id)).map((spec) => spec.dataset))];
  // A tab you may not open — from a shared link, say — falls back to the overview.
  const activeTab = tabs.some((item) => item.key === (tab ?? '')) ? (tab ?? '') : '';

  if (data && !detail && data.error) {
    return (
      <PageBody>
        <Panel className="p-8 text-center">
          <p className="text-sm font-medium">Cannot reach the control plane</p>
          <p className="mt-1 text-[13px] text-[var(--color-ink-muted)]">{data.error}</p>
        </Panel>
      </PageBody>
    );
  }

  return (
    <>
      <PageHeader
        title={detail ? displayName(detail.materialization) : materializationId}
        subtitle={
          detail ? (
            <span className="break-all" title={detail.materialization.source_topics.join('\n')}>
              {topicsLabel(detail.materialization.source_topics)}
            </span>
          ) : (
            'Loading…'
          )
        }
        meta={
          <>
            <Link
              to={domainPath('/materializations')}
              className="inline-flex items-center gap-1 text-[12px] text-[var(--color-ink-muted)] transition-colors hover:text-[var(--color-ink)]"
            >
              <ArrowLeft className="size-3.5" />
              All materializations
            </Link>
            <Identifier value={materializationId} />
            {detail ? (
              <>
                <PhaseBadge
                  phase={detail.status.observed_phase}
                  reason={detail.status.phase_reason}
                />
                <HealthBadge
                  level={detail.status.runtime_health.level}
                  reason={detail.status.runtime_health.message ?? detail.status.runtime_health.reason}
                />
                {detail.status.desired_phase !== detail.status.observed_phase ? (
                  <span className="text-[12px] text-[var(--color-ink-muted)]">
                    Reconciling toward {humanize(detail.status.desired_phase)}
                  </span>
                ) : null}
              </>
            ) : null}
            <LiveIndicator status={status} generatedAt={data?.generatedAt} error={data?.error ?? null} />
          </>
        }
        actions={
          detail ? (
            <>
              {canResume(detail) && can('materializations.resume') ? (
                <Button
                  onClick={() => run('resume', materializationId)}
                  loading={pending === 'resume'}
                >
                  <Play className="size-4" />
                  Resume
                </Button>
              ) : null}
              {canStop(detail) && can('materializations.stop') ? (
                <Button onClick={() => setConfirmStop(true)} loading={pending === 'stop'}>
                  <Square className="size-3.5" />
                  Stop
                </Button>
              ) : null}
              {canEdit(detail) && can('materializations.edit') ? (
                <Button variant="ghost" onClick={() => setEditing(true)}>
                  <Pencil className="size-3.5" />
                  Edit
                </Button>
              ) : null}
              {canDelete(detail) && can('materializations.delete') ? (
                <Button variant="ghost" onClick={() => setConfirmDelete(true)}>
                  <Trash2 className="size-4" />
                  Delete
                </Button>
              ) : null}
            </>
          ) : null
        }
      >
        <div className="mx-auto max-w-[1400px] px-6">
          <nav className="-mb-px flex gap-1" role="tablist">
            {tabs.map((item) => {
              const active = activeTab === item.key;
              return (
                <button
                  key={item.key}
                  role="tab"
                  aria-selected={active}
                  type="button"
                  onClick={() => {
                    // The table chosen on one tab is the one the next shows.
                    const table = searchParams.get(TABLE_PARAM);
                    const query = table ? `?${TABLE_PARAM}=${encodeURIComponent(table)}` : '';
                    navigate(domainPath(`/materializations/${materializationId}${item.key ? `/${item.key}` : ''}${query}`), {
                      replace: true,
                    });
                  }}
                  className={cn(
                    'border-b-2 px-3 pb-2.5 text-[13px] font-medium transition-colors',
                    active
                      ? 'border-[var(--color-ink)] text-[var(--color-ink)]'
                      : 'border-transparent text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]',
                  )}
                >
                  {item.label}
                </button>
              );
            })}
          </nav>
        </div>
      </PageHeader>

      {!data ? (
        <PageBody className="space-y-4">
          <Skeleton className="h-32" />
          <Skeleton className="h-64" />
        </PageBody>
      ) : activeTab === 'schema' ? (
        <SchemaTab materializationId={materializationId} snapshot={data} />
      ) : activeTab === 'data' ? (
        <DataTab materializationId={materializationId} snapshot={data} />
      ) : activeTab === 'table' ? (
        <TableTab snapshot={data} />
      ) : activeTab === 'runtime' ? (
        <RuntimeTab snapshot={data} />
      ) : (
        <DetailOverviewTab snapshot={data} />
      )}

      {detail ? (
        <>
          <EditMaterializationDialog open={editing} onClose={() => setEditing(false)} detail={detail} />
          <Dialog
            open={confirmStop}
            onClose={() => setConfirmStop(false)}
            title={`Stop ${displayName(detail.materialization)}?`}
            description={`Its runtime is scaled to zero replicas: it stops consuming ${topicsLabel(detail.materialization.source_topics, 5)} and writes no new data. Its tables stay readable, and the maintenance app goes on compacting them and expiring old snapshots. Resume scales it back to ${replicaCount(detail.materialization.replicas)}.`}
            footer={
              <>
                <Button variant="ghost" onClick={() => setConfirmStop(false)}>
                  Cancel
                </Button>
                <Button
                  variant="primary"
                  loading={pending === 'stop'}
                  onClick={() => {
                    run('stop', materializationId);
                    setConfirmStop(false);
                  }}
                >
                  Stop
                </Button>
              </>
            }
          >
            <div className="space-y-3 text-[13px] leading-relaxed">
              {/*
                The table's own snapshots record the offsets committed to it, and a runtime resumes
                from them, so a stop loses nothing it had only staged and duplicates nothing.
              */}
              <p className="text-[var(--color-ink-muted)]">
                Everything it already committed stays queryable. A resumed runtime continues from the
                offsets the table records, so rows still being batched when it stops are read again
                and written exactly once.
              </p>
            </div>
          </Dialog>
        </>
      ) : null}

      <Dialog
        open={confirmDelete}
        onClose={() => setConfirmDelete(false)}
        title="Delete materialization?"
        description={`The runtime, its Kafka topics and its consumer group are removed, and so ${
          removed.length > 1 ? `are its ${removed.length} Iceberg tables` : 'is its Iceberg table'
        } with every file in them — unless the control plane has destination cleanup turned off, when the tables leave their datasets but their files stay. The source topics themselves are not touched. This cannot be undone.`}
        footer={
          <>
            <Button variant="ghost" onClick={() => setConfirmDelete(false)}>
              Cancel
            </Button>
            <Button
              variant="danger"
              loading={pending === 'delete'}
              disabled={unwritable.length > 0}
              onClick={() => {
                run('delete', materializationId);
                setConfirmDelete(false);
              }}
            >
              Delete
            </Button>
          </>
        }
      >
        <div className="space-y-3 text-[13px] leading-relaxed">
          <div>
            <p className="text-[var(--color-ink-muted)]">
              {removed.length === 1 ? 'This table leaves its dataset' : 'These tables leave their datasets'}, and anyone’s queries of them
              stop working; their names are free to be used again:
            </p>
            <ul className="mt-1.5 space-y-0.5">
              {removed.map((spec) => (
                <li key={`${spec.dataset}.${spec.name}`} className="flex items-center gap-1.5">
                  <code className="numeric">
                    {spec.dataset}.{spec.name}
                  </code>
                  {can('dataset.write', spec.dataset_id) ? null : <Lock className="size-3 text-[var(--color-ink-faint)]" aria-label="no write access" />}
                </li>
              ))}
            </ul>
          </div>
          {unwritable.length > 0 ? (
            <p className="text-[var(--color-phase-failed)]">
              Deleting needs write access to {unwritable.join(' and ')}, which you do not have: ask {unwritable.length === 1 ? 'its' : 'their'}{' '}
              owners.
            </p>
          ) : (
            <p className="text-[var(--color-ink-muted)]">
              Deletion is asynchronous and multi-stage: runtime teardown, observation confirmation, Kafka cleanup, then
              control-plane record removal.
            </p>
          )}
        </div>
      </Dialog>
    </>
  );
}

function replicaCount(replicas: number | undefined): string {
  const count = replicas ?? 1;
  return `${count} replica${count === 1 ? '' : 's'}`;
}
