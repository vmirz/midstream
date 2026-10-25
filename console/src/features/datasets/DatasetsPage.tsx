import { FolderOpen, Lock, Plus, Search } from 'lucide-react';
import { useDeferredValue, useMemo, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { PageBody, PageHeader } from '@/components/AppShell';
import { Badge, Button, EmptyState, Input, Panel, Skeleton } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useCan } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { formatExact } from '@/lib/format';
import type { CpDataset } from '@shared/types';

import { CreateDatasetDialog } from './CreateDatasetDialog';
import { useDatasets } from './useDatasets';

const GRID = 'grid grid-cols-[minmax(160px,1fr)_minmax(200px,2fr)_minmax(140px,1fr)_80px] items-center gap-4';

/** Whether a dataset matches a search: its name, description or a table's name, without regard to case. */
export function matchesDataset(dataset: Pick<CpDataset, 'name' | 'description' | 'tables'>, query: string): boolean {
  const wanted = query.trim().toLowerCase();
  if (!wanted) return true;
  return [dataset.name, dataset.description, ...dataset.tables].some((value) => value.toLowerCase().includes(wanted));
}

/**
 * Every dataset: what it is for, who owns it and the tables in it. Datasets the person cannot read
 * are listed too, locked: their names and tables are no secret, their schemas and data are.
 */
export function DatasetsPage(): ReactNode {
  const datasets = useDatasets();
  const canCreate = useCan('datasets.create');
  const [search, setSearch] = useState('');
  const [creating, setCreating] = useState(false);
  const query = useDeferredValue(search);
  const items = useMemo(() => (datasets.data?.datasets ?? []).filter((dataset) => matchesDataset(dataset, query)), [datasets.data, query]);

  return (
    <>
      <PageHeader
        title="Datasets"
        subtitle="Named groups of tables: SQL reads each table as <dataset>.<table>. Access is granted per dataset."
        actions={
          canCreate ? (
            <Button variant="primary" onClick={() => setCreating(true)}>
              <Plus className="size-4" />
              New dataset
            </Button>
          ) : null
        }
      >
        <div className="mx-auto flex max-w-[1400px] flex-wrap items-center gap-3 px-6 pb-4">
          <div className="relative min-w-[240px] flex-1">
            <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search by name, description or table"
              className="pl-9"
              aria-label="Search datasets"
            />
          </div>
        </div>
      </PageHeader>

      <PageBody>
        <Panel className="overflow-hidden">
          <div
            className={cn(
              GRID,
              'border-b border-[var(--color-line)] bg-[var(--color-raised)] px-5 py-2 text-[11px] font-medium tracking-wide text-[var(--color-ink-muted)] uppercase',
            )}
          >
            <span>Dataset</span>
            <span>Tables</span>
            <span>Owners</span>
            <span className="text-right">Access</span>
          </div>
          {datasets.isPending ? (
            <div className="space-y-px p-5">
              {[0, 1, 2].map((index) => (
                <Skeleton key={index} className="h-12" />
              ))}
            </div>
          ) : datasets.isError ? (
            <EmptyState
              title="Datasets could not be loaded"
              description={datasets.error instanceof ApiError ? datasets.error.message : 'Try again in a moment.'}
            />
          ) : items.length === 0 ? (
            <EmptyState
              icon={<FolderOpen className="size-6" />}
              title={search ? 'No matches' : 'No datasets yet'}
              description={
                search
                  ? 'Try a different search.'
                  : canCreate
                    ? 'Create one, then put tables in it when you create a materialization.'
                    : 'Someone who may create datasets has not created one yet.'
              }
              action={
                !search && canCreate ? (
                  <Button variant="primary" onClick={() => setCreating(true)}>
                    <Plus className="size-4" />
                    New dataset
                  </Button>
                ) : null
              }
            />
          ) : (
            <ul className="divide-y divide-[var(--color-line)]">
              {items.map((dataset) => (
                <DatasetRow key={dataset.dataset_id} dataset={dataset} />
              ))}
            </ul>
          )}
        </Panel>
      </PageBody>

      <CreateDatasetDialog open={creating} onClose={() => setCreating(false)} />
    </>
  );
}

function DatasetRow({ dataset }: { dataset: CpDataset }): ReactNode {
  return (
    <li>
      <Link to={`/datasets/${encodeURIComponent(dataset.name)}`} className={cn(GRID, 'px-5 py-3 transition-colors hover:bg-[var(--color-raised)]')}>
        <div className="min-w-0">
          <p className="numeric flex items-center gap-1.5 truncate text-[13px] font-medium">
            {dataset.name}
            {dataset.locked ? <Lock className="size-3 shrink-0 text-[var(--color-ink-faint)]" aria-label="locked" /> : null}
          </p>
          <p className="mt-0.5 truncate text-[12px] text-[var(--color-ink-muted)]" title={dataset.description}>
            {dataset.description || '—'}
          </p>
        </div>
        <p className="numeric truncate text-[12px] text-[var(--color-ink-muted)]" title={dataset.tables.join('\n')}>
          {dataset.table_count === 0 ? (
            <span className="text-[var(--color-ink-faint)]">empty</span>
          ) : (
            <>
              <span className="text-[var(--color-ink)]">{formatExact(dataset.table_count)}</span> · {dataset.tables.slice(0, 4).join(', ')}
              {dataset.table_count > 4 ? ', …' : ''}
            </>
          )}
        </p>
        <p className="truncate text-[12px] text-[var(--color-ink-muted)]" title={dataset.owners.map((owner) => owner.email ?? owner.name).join('\n')}>
          {dataset.owners.length ? dataset.owners.map((owner) => owner.name).join(', ') : '—'}
        </p>
        <span className="text-right">
          <AccessBadge dataset={dataset} />
        </span>
      </Link>
    </li>
  );
}

/** What the person may do with a dataset, in a word. */
export function AccessBadge({ dataset }: { dataset: Pick<CpDataset, 'permissions' | 'locked' | 'name'> }): ReactNode {
  if (dataset.permissions.includes('dataset.manage')) return <Badge tone="accent">owner</Badge>;
  if (dataset.permissions.includes('dataset.write')) return <Badge tone="running">writer</Badge>;
  if (!dataset.locked) return <Badge>reader</Badge>;
  return (
    <Badge title={`Its schemas and data need a role on ${dataset.name}: ask one of its owners.`}>
      <Lock className="size-3" />
      locked
    </Badge>
  );
}
