import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, Lock, Pencil, Trash2 } from 'lucide-react';
import { useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';

import { PageBody, PageHeader } from '@/components/AppShell';
import { RelativeTime } from '@/components/Inline';
import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, EmptyState, Field, Input, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { ApiError, api } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import type { CpDatasetDetail, CpDatasetTable, DatasetTableState } from '@shared/types';

import { ConfirmDialog } from '../access/ConfirmDialog';
import { DatasetAccess } from './DatasetAccess';
import { AccessBadge } from './DatasetsPage';
import { useRefreshDatasets } from './useDatasets';

const TABS = [
  { key: 'tables', label: 'Tables' },
  { key: 'access', label: 'Access' },
] as const;

type Tab = (typeof TABS)[number]['key'];

/** Read a dataset again: after a change to it, its access, or its tables. */
export function datasetKey(name: string): readonly unknown[] {
  return ['dataset', name];
}

/**
 * One dataset: what it is for, who owns it, and its tables with the materializations that write
 * them. Its owners edit its description, decide who may use it (Access), and delete it once it holds
 * no tables.
 */
export function DatasetPage(): ReactNode {
  const { name = '', tab } = useParams<{ name: string; tab?: string }>();
  const navigate = useNavigate();
  const { can } = useAuth();
  const [editing, setEditing] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const dataset = useQuery({ queryKey: datasetKey(name), queryFn: () => api.dataset(name), refetchInterval: 15_000, retry: 0 });
  const data = dataset.data ?? null;
  const manages = data !== null && can('dataset.manage', data.dataset_id);
  const tabs = TABS.filter((item) => item.key !== 'access' || manages);
  const activeTab: Tab = tabs.some((item) => item.key === tab) ? (tab as Tab) : 'tables';

  if (dataset.isError) {
    const missing = dataset.error instanceof ApiError && dataset.error.status === 404;
    return (
      <PageBody>
        <Panel>
          <EmptyState
            title={missing ? `There is no dataset ${name}` : 'The dataset could not be loaded'}
            description={dataset.error instanceof ApiError ? dataset.error.message : 'Try again in a moment.'}
            action={
              <Link to="/datasets" className="text-[13px] text-[var(--color-accent)] hover:underline">
                All datasets
              </Link>
            }
          />
        </Panel>
      </PageBody>
    );
  }

  return (
    <>
      <PageHeader
        title={<span className="numeric">{name}</span>}
        subtitle={data ? data.description || 'No description.' : 'Loading…'}
        meta={
          <>
            <Link
              to="/datasets"
              className="inline-flex items-center gap-1 text-[12px] text-[var(--color-ink-muted)] transition-colors hover:text-[var(--color-ink)]"
            >
              <ArrowLeft className="size-3.5" />
              All datasets
            </Link>
            {data ? (
              <>
                <AccessBadge dataset={data} />
                <span className="text-[12px] text-[var(--color-ink-muted)]">
                  Owned by {data.owners.length ? data.owners.map((owner) => owner.name).join(', ') : 'nobody'}
                </span>
              </>
            ) : null}
          </>
        }
        actions={
          manages ? (
            <>
              <Button variant="ghost" onClick={() => setEditing(true)}>
                <Pencil className="size-3.5" />
                Edit
              </Button>
              <Button variant="ghost" onClick={() => setDeleting(true)}>
                <Trash2 className="size-4" />
                Delete
              </Button>
            </>
          ) : null
        }
      >
        <div className="mx-auto max-w-[1400px] px-6">
          <nav className="-mb-px flex gap-1" role="tablist">
            {tabs.map((item) => (
              <button
                key={item.key}
                role="tab"
                type="button"
                aria-selected={activeTab === item.key}
                onClick={() => navigate(`/datasets/${encodeURIComponent(name)}${item.key === 'tables' ? '' : `/${item.key}`}`, { replace: true })}
                className={cn(
                  'border-b-2 px-3 pb-2.5 text-[13px] font-medium transition-colors',
                  activeTab === item.key
                    ? 'border-[var(--color-ink)] text-[var(--color-ink)]'
                    : 'border-transparent text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]',
                )}
              >
                {item.label}
              </button>
            ))}
          </nav>
        </div>
      </PageHeader>

      {!data ? (
        <PageBody className="space-y-4">
          <Skeleton className="h-48" />
        </PageBody>
      ) : activeTab === 'access' ? (
        <DatasetAccess dataset={data} />
      ) : (
        <DatasetTables dataset={data} />
      )}

      {data ? (
        <>
          <EditDatasetDialog open={editing} onClose={() => setEditing(false)} dataset={data} />
          <DeleteDatasetDialog open={deleting} onClose={() => setDeleting(false)} dataset={data} />
        </>
      ) : null}
    </>
  );
}

const STATE_LABELS: Record<DatasetTableState, { label: string; tone: 'running' | 'pending' | 'neutral' }> = {
  pending: { label: 'being created', tone: 'pending' },
  active: { label: 'written', tone: 'running' },
  retired: { label: 'kept · read-only', tone: 'neutral' },
  dropping: { label: 'being dropped', tone: 'pending' },
  dropped: { label: 'dropped', tone: 'neutral' },
};

function DatasetTables({ dataset }: { dataset: CpDatasetDetail }): ReactNode {
  const { can } = useAuth();
  const readable = !dataset.locked;
  return (
    <PageBody className="space-y-4">
      {dataset.locked ? (
        <p className="flex items-start gap-2 rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] px-4 py-3 text-[12px] text-[var(--color-ink-muted)]">
          <Lock className="mt-0.5 size-3.5 shrink-0" />
          This dataset is locked to you: you see its tables’ names, not their schemas or data. Ask one of its owners for a
          role on it.
        </p>
      ) : null}
      <Panel className="overflow-hidden">
        <PanelHeader
          title={`Tables (${dataset.tables.filter((table) => table.state !== 'dropping').length})`}
          description={
            <>
              SQL reads each as <code className="numeric">{dataset.name}.&lt;table&gt;</code>, and engines on the Iceberg catalog as{' '}
              <code className="numeric">lake.{dataset.name}.&lt;table&gt;</code>. Each is written by one materialization.
            </>
          }
        />
        {dataset.tables.length === 0 ? (
          <EmptyState
            title="No tables yet"
            description={
              can('dataset.write', dataset.dataset_id)
                ? 'Choose this dataset for a topic’s table when you create or edit a materialization.'
                : 'Tables are added by people who may write to this dataset.'
            }
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[640px] text-[12px]">
              <thead>
                <tr className="border-b border-[var(--color-line)] bg-[var(--color-raised)] text-left text-[11px] tracking-wide text-[var(--color-ink-muted)] uppercase">
                  <th className="px-5 py-2 font-medium">Table · topic</th>
                  <th className="px-2 py-2 font-medium">Materialization</th>
                  <th className="px-2 py-2 font-medium">State</th>
                  <th className="px-5 py-2 font-medium">Last commit</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--color-line)]">
                {dataset.tables.map((table) => (
                  <DatasetTableRow key={`${table.materialization_id}:${table.name}`} dataset={dataset.name} table={table} readable={readable} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </PageBody>
  );
}

function DatasetTableRow({ dataset, table, readable }: { dataset: string; table: CpDatasetTable; readable: boolean }): ReactNode {
  const { can } = useAuth();
  const state = STATE_LABELS[table.state] ?? { label: table.state, tone: 'neutral' as const };
  const detail = `/materializations/${encodeURIComponent(table.materialization_id)}`;
  const tableQuery = `?table=${encodeURIComponent(table.name)}`;
  const canOpen = can('materializations.view_details');
  return (
    <tr className={cn('align-top', table.state === 'dropping' && 'opacity-60')}>
      <td className="px-5 py-2">
        {canOpen && readable && (table.state === 'active' || table.state === 'retired') ? (
          <Link to={`${detail}/table${tableQuery}`} className="numeric font-medium hover:text-[var(--color-accent)] hover:underline">
            {dataset}.{table.name}
          </Link>
        ) : (
          <span className="numeric font-medium">
            {dataset}.{table.name}
          </span>
        )}
        <span className="numeric block break-all text-[11px] text-[var(--color-ink-muted)]">{table.source_topic}</span>
      </td>
      <td className="px-2 py-2">
        {canOpen && table.materialization_name !== null ? (
          <Link to={detail} className="hover:text-[var(--color-accent)] hover:underline">
            {table.materialization_name}
          </Link>
        ) : (
          <span>{table.materialization_name ?? table.materialization_id}</span>
        )}
        <span className="numeric block text-[11px] text-[var(--color-ink-faint)]">{table.materialization_id}</span>
      </td>
      <td className="px-2 py-2">
        <Badge tone={state.tone}>{state.label}</Badge>
      </td>
      <td className="px-5 py-2 whitespace-nowrap text-[var(--color-ink-muted)]">
        <RelativeTime value={table.last_commit_at} />
      </td>
    </tr>
  );
}

function EditDatasetDialog({ open, onClose, dataset }: { open: boolean; onClose: () => void; dataset: CpDatasetDetail }): ReactNode {
  const [description, setDescription] = useState(dataset.description);
  const [error, setError] = useState<string | null>(null);
  const queryClient = useQueryClient();
  const refresh = useRefreshDatasets();
  const toast = useToast();

  useEffect(() => {
    if (open) {
      setDescription(dataset.description);
      setError(null);
    }
  }, [open, dataset.description]);

  const save = useMutation({
    mutationFn: () => api.updateDataset(dataset.name, { description: description.trim(), base_revision: dataset.revision }),
    onSuccess: async () => {
      await Promise.all([queryClient.invalidateQueries({ queryKey: datasetKey(dataset.name) }), refresh()]);
      toast.success('Dataset changed', `${dataset.name}’s description is saved.`);
      onClose();
    },
    onError: (failure: unknown) => setError(failure instanceof ApiError ? failure.message : 'The change could not be saved.'),
  });

  const onSubmit = (event: FormEvent): void => {
    event.preventDefault();
    save.mutate();
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Edit ${dataset.name}`}
      description="A dataset’s name never changes; its description can."
      footer={
        <>
          <Button variant="ghost" type="button" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="edit-dataset" loading={save.isPending} disabled={description.trim() === dataset.description}>
            Save
          </Button>
        </>
      }
    >
      <form id="edit-dataset" onSubmit={onSubmit} className="space-y-3">
        <Field label="Description" error={error}>
          <Input autoFocus value={description} onChange={(event) => setDescription(event.target.value)} maxLength={500} />
        </Field>
      </form>
    </Dialog>
  );
}

function DeleteDatasetDialog({ open, onClose, dataset }: { open: boolean; onClose: () => void; dataset: CpDatasetDetail }): ReactNode {
  const navigate = useNavigate();
  const refresh = useRefreshDatasets();
  const toast = useToast();
  const remove = useMutation({
    mutationFn: () => api.deleteDataset(dataset.name),
    onSuccess: async () => {
      await refresh();
      toast.success('Dataset deleted', `${dataset.name} is gone; its name can be used again.`);
      onClose();
      navigate('/datasets');
    },
    onError: (failure: unknown) => toast.error('Could not delete the dataset', failure instanceof ApiError ? failure.message : 'Try again in a moment.'),
  });
  const held = dataset.tables.length;
  return (
    <ConfirmDialog
      open={open}
      onClose={onClose}
      title={`Delete ${dataset.name}?`}
      description={
        held > 0
          ? `It still holds ${held} ${held === 1 ? 'table' : 'tables'}: a dataset can be deleted only once its tables are gone - dropped, or their materializations deleted.`
          : 'Its grants go with it, and its name can be used again. This cannot be undone.'
      }
      confirmLabel={held > 0 ? 'Close' : 'Delete'}
      danger={held === 0}
      pending={remove.isPending}
      onConfirm={() => (held > 0 ? onClose() : remove.mutate())}
    />
  );
}
