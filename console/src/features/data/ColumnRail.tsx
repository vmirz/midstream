import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronRight, Pin, Search } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { ColumnLabelText } from './ColumnLabelText';
import { compactType, groupColumns, type ColumnGroupId, type ColumnLabel } from './columns';
import { Input, Skeleton } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { formatCount } from '@/lib/format';
import { useApi } from '@/lib/domains';
import type { BrowseFilter, QueryColumn } from '@shared/types';

/**
 * Left rail listing every column in the current result, grouped and searchable.
 *
 * This is the tab's primary navigation: with 25+ columns the grid alone cannot tell you what is
 * in the dataset, so the rail carries visibility, an on-demand value profile, and one-click
 * filtering from the profile's top values.
 */
export function ColumnRail({
  columns,
  labels,
  schemaOrder,
  visible,
  onToggle,
  onShowOnly,
  onSetGroup,
  pinnedColumn,
  onPin,
  runId,
  onFilter,
  width,
}: {
  columns: QueryColumn[];
  labels: Map<string, ColumnLabel>;
  schemaOrder: string[];
  visible: Set<string>;
  onToggle: (column: string) => void;
  onShowOnly: (column: string) => void;
  onSetGroup: (group: ColumnGroupId, visible: boolean) => void;
  pinnedColumn: string | null;
  onPin: (column: string | null) => void;
  /** The run whose rows profiles describe; null before one exists. */
  runId: string | null;
  /** Rail width in pixels, owned by the caller so the drag handle beside it can set it. */
  width: number;
  /** Absent when filters cannot apply, as under a hand-written statement. */
  onFilter?: (filter: BrowseFilter) => void;
}): ReactNode {
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState<Set<ColumnGroupId>>(new Set());

  const groups = useMemo(() => groupColumns(columns, schemaOrder), [columns, schemaOrder]);
  const needle = search.trim().toLowerCase();

  const filteredGroups = useMemo(
    () =>
      groups
        .map((group) => ({
          ...group,
          columns: needle
            ? group.columns.filter((column) => column.name.toLowerCase().includes(needle))
            : group.columns,
        }))
        .filter((group) => group.columns.length > 0),
    [groups, needle],
  );

  return (
    // Width is inline rather than a class because it is a dragged value: a Tailwind arbitrary
    // value would mint a new class on every pixel of the drag.
    <div
      style={{ width }}
      className="flex h-full min-h-0 shrink-0 flex-col border-r border-[var(--color-line)]"
    >
      <div className="border-b border-[var(--color-line)] p-2.5">
        <div className="relative">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-[var(--color-ink-faint)]" />
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Find a column"
            spellCheck={false}
            aria-label="Find a column"
            className="h-8 pl-8 text-[12px]"
          />
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto py-1">
        {/*
          An empty rail and an unmatched search read very differently, and quoting an empty search
          back at the reader — `No column matches ""` — describes neither.
        */}
        {filteredGroups.length === 0 ? (
          <p className="px-3 py-6 text-center text-[12px] text-[var(--color-ink-muted)]">
            {search.trim() === ''
              ? 'Columns appear once a query returns rows.'
              : `No column matches “${search}”.`}
          </p>
        ) : null}

        {filteredGroups.map((group) => {
          const isCollapsed = collapsed.has(group.id) && !needle;
          const shownCount = group.columns.filter((column) => visible.has(column.name)).length;
          const allShown = shownCount === group.columns.length;

          return (
            <section key={group.id} className="mb-1">
              <div className="flex items-center gap-1 px-2 py-1">
                <button
                  type="button"
                  onClick={() =>
                    setCollapsed((current) => {
                      const next = new Set(current);
                      if (next.has(group.id)) next.delete(group.id);
                      else next.add(group.id);
                      return next;
                    })
                  }
                  className="flex min-w-0 flex-1 items-center gap-1 text-left"
                  title={group.hint}
                >
                  {isCollapsed ? (
                    <ChevronRight className="size-3 shrink-0 text-[var(--color-ink-faint)]" />
                  ) : (
                    <ChevronDown className="size-3 shrink-0 text-[var(--color-ink-faint)]" />
                  )}
                  <span className="truncate text-[11px] font-semibold tracking-wide text-[var(--color-ink-muted)] uppercase">
                    {group.label}
                  </span>
                  <span className="numeric shrink-0 text-[11px] text-[var(--color-ink-faint)]">
                    {shownCount}/{group.columns.length}
                  </span>
                </button>
                <button
                  type="button"
                  onClick={() => onSetGroup(group.id, !allShown)}
                  className="shrink-0 rounded px-1.5 py-0.5 text-[11px] text-[var(--color-ink-faint)] transition-colors hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]"
                >
                  {allShown ? 'None' : 'All'}
                </button>
              </div>

              {isCollapsed ? null : (
                <ul>
                  {group.columns.map((column) => (
                    <ColumnRow
                      key={column.name}
                      column={column}
                      labels={labels}
                      visible={visible.has(column.name)}
                      expanded={expanded === column.name}
                      onToggleVisible={() => onToggle(column.name)}
                      onToggleExpanded={() =>
                        setExpanded((current) => (current === column.name ? null : column.name))
                      }
                      onShowOnly={() => onShowOnly(column.name)}
                      pinned={pinnedColumn === column.name}
                      onPin={() => onPin(pinnedColumn === column.name ? null : column.name)}
                      runId={runId}
                      onFilter={onFilter}
                    />
                  ))}
                </ul>
              )}
            </section>
          );
        })}
      </div>
    </div>
  );
}

function ColumnRow({
  column,
  labels,
  visible,
  expanded,
  onToggleVisible,
  onToggleExpanded,
  onShowOnly,
  pinned,
  onPin,
  runId,
  onFilter,
}: {
  column: QueryColumn;
  labels: Map<string, ColumnLabel>;
  visible: boolean;
  expanded: boolean;
  onToggleVisible: () => void;
  onToggleExpanded: () => void;
  onShowOnly: () => void;
  pinned: boolean;
  onPin: () => void;
  runId: string | null;
  onFilter?: (filter: BrowseFilter) => void;
}): ReactNode {
  return (
    <li>
      <div
        className={cn(
          'group flex items-center gap-1.5 px-2 py-1 transition-colors hover:bg-[var(--color-raised)]',
          expanded && 'bg-[var(--color-raised)]',
        )}
      >
        <input
          type="checkbox"
          checked={visible}
          onChange={onToggleVisible}
          aria-label={`Show ${column.name}`}
          className="size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
        />
        <button
          type="button"
          onClick={onToggleExpanded}
          className="min-w-0 flex-1 text-left"
        >
          <ColumnLabelText
            labels={labels}
            name={column.name}
            className={cn(
              'numeric text-[12px]',
              visible ? 'text-[var(--color-ink)]' : 'text-[var(--color-ink-faint)]',
            )}
          />
        </button>
        <button
          type="button"
          onClick={onPin}
          title={pinned ? 'Unfreeze this column' : 'Freeze this column to the left'}
          aria-label={pinned ? `Unfreeze ${column.name}` : `Freeze ${column.name}`}
          aria-pressed={pinned}
          className={cn(
            'shrink-0 rounded p-0.5 transition-opacity hover:text-[var(--color-ink)]',
            pinned
              ? 'text-[var(--color-accent)] opacity-100'
              : 'text-[var(--color-ink-faint)] opacity-0 group-hover:opacity-100',
          )}
        >
          <Pin className={cn('size-3', pinned && 'fill-current')} />
        </button>
        <button
          type="button"
          onClick={onShowOnly}
          title="Show only this column"
          className="shrink-0 rounded px-1 text-[10px] text-[var(--color-ink-faint)] opacity-0 transition-opacity group-hover:opacity-100 hover:text-[var(--color-ink)]"
        >
          only
        </button>
        <span className="shrink-0 text-[10px] tracking-wide text-[var(--color-ink-faint)] uppercase">
          {compactType(column.type)}
        </span>
      </div>

      {expanded ? <ColumnProfilePanel column={column} runId={runId} onFilter={onFilter} /> : null}
    </li>
  );
}

/**
 * Inline distribution for one column, fetched only when the row is expanded.
 *
 * Profiles describe the rows of the run on screen — its time range and filters, or a statement's
 * result — which is what makes the top values usable as filters.
 */
function ColumnProfilePanel({
  column,
  runId,
  onFilter,
}: {
  column: QueryColumn;
  runId: string | null;
  onFilter?: (filter: BrowseFilter) => void;
}): ReactNode {
  const api = useApi();
  const profileQuery = useQuery({
    queryKey: ['column-profile', runId, column.name],
    queryFn: () => api.runProfile(runId as string, column.name),
    enabled: runId !== null,
    retry: 0,
    staleTime: Number.POSITIVE_INFINITY,
  });

  if (profileQuery.isPending) {
    return (
      <div className="space-y-1.5 border-y border-[var(--color-line)] bg-[var(--color-canvas)] px-3 py-2.5">
        <Skeleton className="h-3 w-3/4" />
        <Skeleton className="h-3 w-1/2" />
        <Skeleton className="h-3 w-2/3" />
      </div>
    );
  }

  if (profileQuery.isError || !profileQuery.data) {
    return (
      <div className="border-y border-[var(--color-line)] bg-[var(--color-canvas)] px-3 py-2.5 text-[11px] text-[var(--color-ink-muted)]">
        Profile unavailable for this column.
      </div>
    );
  }

  const profile = profileQuery.data;
  if (profile.partial && profile.topValues.length === 0 && profile.total === 0) {
    return (
      <div className="border-y border-[var(--color-line)] bg-[var(--color-canvas)] px-3 py-2.5 text-[11px] text-[var(--color-ink-muted)]">
        Too much data to profile quickly. Narrow the time range, then reopen.
      </div>
    );
  }
  const nullCount = profile.total - profile.nonNull;
  const nullPercent = profile.total > 0 ? Math.round((nullCount / profile.total) * 100) : 0;
  const maxCount = Math.max(1, ...profile.topValues.map((entry) => entry.count));

  return (
    <div className="space-y-2 border-y border-[var(--color-line)] bg-[var(--color-canvas)] px-3 py-2.5">
      <div className="numeric flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] text-[var(--color-ink-muted)]">
        <span title="Approximate distinct values">{formatCount(profile.distinct)} distinct</span>
        <span className={nullCount > 0 ? 'text-[var(--color-phase-paused)]' : undefined}>
          {nullPercent}% null
        </span>
        {profile.partial ? (
          <span
            className="text-[var(--color-phase-paused)]"
            title="One of the profile queries ran out of time; these figures are incomplete."
          >
            partial
          </span>
        ) : null}
      </div>

      {profile.topValues.length > 0 ? (
        <ul className="space-y-0.5">
          {profile.topValues.slice(0, 6).map((entry, index) => (
            <li key={`${entry.value ?? 'null'}-${index}`}>
              <button
                type="button"
                disabled={entry.value === null || !onFilter}
                onClick={() =>
                  onFilter?.({ column: column.name, operator: 'eq', value: entry.value ?? '' })
                }
                title={entry.value === null ? 'null' : onFilter ? `Filter to ${entry.value}` : entry.value}
                className="relative block w-full overflow-hidden rounded px-1.5 py-0.5 text-left transition-colors hover:bg-[var(--color-raised)] disabled:cursor-default"
              >
                <span
                  aria-hidden
                  className="absolute inset-y-0 left-0 bg-[var(--color-accent-soft)]"
                  style={{ width: `${(entry.count / maxCount) * 100}%` }}
                />
                <span className="numeric relative flex items-baseline justify-between gap-2 text-[11px]">
                  <span
                    className={cn(
                      'truncate',
                      entry.value === null
                        ? 'text-[var(--color-ink-faint)] italic'
                        : 'text-[var(--color-ink)]',
                    )}
                  >
                    {entry.value === null ? 'null' : entry.value}
                  </span>
                  <span className="shrink-0 text-[var(--color-ink-muted)]">
                    {formatCount(entry.count)}
                  </span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      ) : null}

      {profile.min !== null && profile.max !== null && profile.distinct > 6 ? (
        <p className="numeric truncate text-[11px] text-[var(--color-ink-muted)]">
          {profile.min} → {profile.max}
        </p>
      ) : null}

    </div>
  );
}
