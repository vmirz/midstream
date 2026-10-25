import { ArrowDown, ArrowUp, Plus, Search } from 'lucide-react';
import { useDeferredValue, useMemo, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { CreateMaterializationDialog } from './CreateMaterializationDialog';
import { LiveIndicator, PageBody, PageHeader } from '@/components/AppShell';
import { RelativeTime } from '@/components/Inline';
import { PhaseBadge } from '@/components/PhaseBadge';
import {
  Button,
  EmptyState,
  Input,
  Panel,
  SegmentedControl,
  Skeleton,
} from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { displayName, formatBytes, formatCount, formatRate, topicsLabel } from '@/lib/format';
import { useCan } from '@/lib/auth';
import { useFleetStream } from '@/lib/live';
import { isDeletedPhase, type MaterializationSummary } from '@shared/types';

type FilterKey = 'all' | 'running' | 'pending' | 'stopped' | 'attention' | 'deleted';
type SortKey = 'updated' | 'name' | 'rows' | 'rate' | 'bytes';

const FILTERS: { value: FilterKey; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'running', label: 'Running' },
  { value: 'pending', label: 'Pending' },
  { value: 'stopped', label: 'Stopped' },
  { value: 'attention', label: 'Attention' },
];

const DELETED_FILTER: FilterKey = 'deleted';

const PENDING = new Set(['requested', 'discovering', 'discovered', 'deploying']);
/** `paused` is what control planes recorded before Stop replaced Pause. */
const STOPPED = new Set(['stopped', 'paused']);
const ATTENTION = new Set(['failed', 'degraded']);

const COLUMNS: { key: SortKey | null; label: string; className: string }[] = [
  { key: 'name', label: 'Materialization', className: 'text-left' },
  { key: null, label: 'Phase', className: 'text-left' },
  { key: 'rate', label: 'Rate', className: 'text-right' },
  { key: 'rows', label: 'Rows', className: 'text-right' },
  { key: 'bytes', label: 'Size', className: 'text-right' },
  { key: null, label: 'Tables', className: 'text-right' },
  { key: 'updated', label: 'Updated', className: 'text-right' },
];

const GRID = 'grid grid-cols-[minmax(0,2.4fr)_150px_110px_100px_100px_80px_120px] items-center gap-4';

export function MaterializationsPage(): ReactNode {
  const { data, status } = useFleetStream();
  const [createOpen, setCreateOpen] = useState(false);
  const canCreate = useCan('materializations.create');
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState<FilterKey>('all');
  const [sort, setSort] = useState<{ key: SortKey; direction: 'asc' | 'desc' }>({
    key: 'updated',
    direction: 'desc',
  });

  const deferredSearch = useDeferredValue(search);

  const filterOptions = useMemo(() => {
    const deletedCount = data?.totals.deleted ?? 0;
    return [
      ...FILTERS,
      {
        value: DELETED_FILTER,
        label: deletedCount > 0 ? `Recently deleted (${deletedCount})` : 'Recently deleted',
      },
    ];
  }, [data?.totals.deleted]);

  const rows = useMemo(() => {
    const query = deferredSearch.trim().toLowerCase();
    const filtered = (data?.items ?? []).filter((item) => {
      // Deleted materializations are only surfaced through the explicit filter.
      if (filter === 'deleted') {
        if (!isDeletedPhase(item)) return false;
      } else if (isDeletedPhase(item)) {
        return false;
      }
      if (filter === 'running' && item.observed_phase !== 'running') return false;
      if (filter === 'pending' && !PENDING.has(item.observed_phase)) return false;
      if (filter === 'stopped' && !STOPPED.has(item.observed_phase)) return false;
      if (
        filter === 'attention' &&
        !ATTENTION.has(item.observed_phase) &&
        item.desired_phase === item.observed_phase
      ) {
        return false;
      }
      if (!query) return true;
      // What a row shows is what a search matches: its name, id, source topics, datasets and tables
      // (`dataset.table`).
      return [item.materialization_id, item.name, ...item.source_topics, ...(item.datasets ?? []), ...item.tables]
        .filter((value): value is string => Boolean(value))
        .some((value) => value.toLowerCase().includes(query));
    });

    const direction = sort.direction === 'asc' ? 1 : -1;
    return filtered.sort((a, b) => {
      switch (sort.key) {
        case 'name':
          return displayName(a).localeCompare(displayName(b)) * direction;
        case 'rows':
          return ((a.rowsCommitted ?? 0) - (b.rowsCommitted ?? 0)) * direction;
        case 'rate':
          return ((a.rowsPerSecond ?? 0) - (b.rowsPerSecond ?? 0)) * direction;
        case 'bytes':
          return ((a.tableBytes ?? 0) - (b.tableBytes ?? 0)) * direction;
        default:
          return (a.updated_at ?? '').localeCompare(b.updated_at ?? '') * direction;
      }
    });
  }, [data?.items, deferredSearch, filter, sort]);

  const toggleSort = (key: SortKey): void => {
    setSort((current) =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: key === 'name' ? 'asc' : 'desc' },
    );
  };

  return (
    <>
      <PageHeader
        title="Materializations"
        subtitle={
          data
            ? `${data.totals.materializations} total · ${data.totals.running} running · ${formatRate(data.totals.rowsPerSecond, 'rows/s')}`
            : 'Loading…'
        }
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
      >
        <div className="mx-auto flex max-w-[1400px] flex-wrap items-center gap-3 px-6 pb-4">
          <div className="relative min-w-[240px] flex-1">
            <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search by name, id, topic, dataset or table"
              className="pl-9"
              spellCheck={false}
            />
          </div>
          <SegmentedControl value={filter} options={filterOptions} onChange={setFilter} />
        </div>
      </PageHeader>

      <PageBody>
        <Panel className="overflow-hidden">
          <div
            className={cn(
              GRID,
              'border-b border-[var(--color-line)] bg-[var(--color-raised)] px-5 py-2.5',
              'text-[11px] font-medium tracking-wide text-[var(--color-ink-muted)] uppercase',
            )}
          >
            {COLUMNS.map((column) => (
              <button
                key={column.label}
                type="button"
                disabled={column.key === null}
                onClick={() => column.key && toggleSort(column.key)}
                className={cn(
                  'flex items-center gap-1 truncate',
                  column.className === 'text-right' && 'justify-end',
                  column.key && 'transition-colors hover:text-[var(--color-ink)]',
                )}
              >
                {column.label}
                {column.key && sort.key === column.key ? (
                  sort.direction === 'asc' ? (
                    <ArrowUp className="size-3" />
                  ) : (
                    <ArrowDown className="size-3" />
                  )
                ) : null}
              </button>
            ))}
          </div>

          {!data ? (
            <div className="space-y-px p-5">
              {Array.from({ length: 6 }, (_, index) => (
                <Skeleton key={index} className="h-12" />
              ))}
            </div>
          ) : rows.length === 0 ? (
            <EmptyState
              title={
                filter === 'deleted' && !search
                  ? 'No recently deleted materializations'
                  : search || filter !== 'all'
                    ? 'No matches'
                    : 'No materializations yet'
              }
              description={
                filter === 'deleted' && !search
                  ? 'Deleted materializations stay listed here until the retention window elapses.'
                  : search || filter !== 'all'
                    ? 'Try a different search or filter.'
                    : 'Create one to start writing a Kafka topic into partitioned parquet.'
              }
              action={
                search || filter !== 'all' ? (
                  <Button
                    onClick={() => {
                      setSearch('');
                      setFilter('all');
                    }}
                  >
                    Clear filters
                  </Button>
                ) : canCreate ? (
                  <Button variant="primary" onClick={() => setCreateOpen(true)}>
                    <Plus className="size-4" />
                    New materialization
                  </Button>
                ) : null
              }
            />
          ) : (
            <ul className="divide-y divide-[var(--color-line)]">
              {rows.map((item) => (
                <MaterializationRow key={item.materialization_id} item={item} />
              ))}
            </ul>
          )}
        </Panel>
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

function MaterializationRow({ item }: { item: MaterializationSummary }): ReactNode {
  return (
    <li>
      <Link
        to={`/materializations/${item.materialization_id}`}
        className={cn(GRID, 'px-5 py-3 transition-colors hover:bg-[var(--color-raised)]')}
      >
        <div className="min-w-0">
          <p className="truncate text-[13px] font-medium">{displayName(item)}</p>
          <p className="mt-0.5 truncate text-[12px] text-[var(--color-ink-muted)]" title={item.source_topics.join('\n')}>
            {topicsLabel(item.source_topics, 2)}
            {item.datasets?.length ? (
              <span className="text-[var(--color-ink-faint)]" title={item.tables.join('\n')}>
                {' → '}
                {item.datasets.join(', ')}
              </span>
            ) : null}
          </p>
        </div>

        <div className="flex min-w-0 flex-col items-start gap-1">
          <PhaseBadge phase={item.observed_phase} reason={item.phase_reason} />
          {item.desired_phase !== item.observed_phase ? (
            <span className="truncate text-[11px] text-[var(--color-ink-faint)]">→ {item.desired_phase}</span>
          ) : null}
        </div>

        <span className="numeric text-right text-[12px]">{formatRate(item.rowsPerSecond, 'rows/s')}</span>
        <span className="numeric text-right text-[12px] text-[var(--color-ink-muted)]">
          {formatCount(item.rowsCommitted)}
        </span>
        <span className="numeric text-right text-[12px] text-[var(--color-ink-muted)]">
          {formatBytes(item.tableBytes)}
        </span>
        <span className="numeric text-right text-[12px] text-[var(--color-ink-muted)]">
          {formatCount(item.tables.length)}
        </span>
        <span className="text-right text-[12px] text-[var(--color-ink-muted)]">
          <RelativeTime value={item.updated_at} />
        </span>
      </Link>
    </li>
  );
}
