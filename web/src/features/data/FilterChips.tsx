import { useQuery } from '@tanstack/react-query';
import { Filter, Plus, Search, X } from 'lucide-react';
import { useDeferredValue, useEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react';

import { ColumnLabelText } from './ColumnLabelText';
import { compactType, groupOf, isComparableType, type ColumnLabel } from './columns';
import { searchFields } from './fieldSearch';
import { Button, Input, Select, Spinner } from '@/components/ui/primitives';
import { Popover } from '@/components/ui/Popover';
import { api } from '@/lib/api';
import { cn } from '@/lib/cn';
import { formatCount } from '@/lib/format';
import { segmentName } from '@/lib/nameSearch';
import type { BrowseFilter, FilterOperator, QueryColumn } from '@shared/types';

const OPERATORS: { value: FilterOperator; label: string; unary?: boolean; comparable?: boolean }[] = [
  { value: 'eq', label: 'is' },
  { value: 'neq', label: 'is not' },
  { value: 'contains', label: 'contains' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'gt', label: '>', comparable: true },
  { value: 'gte', label: '≥', comparable: true },
  { value: 'lt', label: '<', comparable: true },
  { value: 'lte', label: '≤', comparable: true },
  { value: 'is_null', label: 'is null', unary: true },
  { value: 'is_not_null', label: 'is not null', unary: true },
];

const OPERATOR_LABEL = new Map(OPERATORS.map((entry) => [entry.value, entry.label]));

function isUnary(operator: FilterOperator): boolean {
  return OPERATORS.find((entry) => entry.value === operator)?.unary === true;
}

/**
 * Active filters as removable chips, plus the popover that builds a new one.
 *
 * The builder profiles the chosen column as soon as it is picked, so the common case — filter to
 * one of a handful of known values — becomes a click on a real value rather than typing a guess.
 */
export function FilterChips({
  columns,
  labels,
  filters,
  onChange,
  runId,
  superseded = false,
}: {
  columns: QueryColumn[];
  labels: Map<string, ColumnLabel>;
  filters: BrowseFilter[];
  onChange: (filters: BrowseFilter[]) => void;
  /** The run whose values the builder suggests; null before one exists. */
  runId: string | null;
  /**
   * A hand-written statement now defines the query, so these are not being applied.
   *
   * Shown rather than hidden, because **Revert** brings them back and they are worth seeing in the
   * meantime — but drained of colour and inert, because a chip that looks live and does nothing is
   * worse than no chip at all. The console does not try to read the statement's `WHERE` back into
   * chips: a statement can say things no chip can, and a round trip that silently drops an `OR` or
   * a function call would be a wrong answer disguised as a convenience.
   */
  superseded?: boolean;
}): ReactNode {
  return (
    <>
      {filters.map((filter, index) => (
        <span
          key={`${filter.column}-${filter.operator}-${index}`}
          title={
            superseded
              ? 'Not applied — the statement defines the query. Revert to use these filters again.'
              : undefined
          }
          className={cn(
            'numeric inline-flex h-8 shrink-0 items-center gap-1.5 rounded-[var(--radius-control)] px-2 text-[11px]',
            superseded
              ? 'border border-dashed border-[var(--color-line-strong)] text-[var(--color-ink-faint)] opacity-60'
              : 'border border-[var(--color-accent)] bg-[var(--color-accent-soft)]',
          )}
        >
          <ColumnLabelText
            labels={labels}
            name={filter.column}
            className="max-w-[160px] text-[var(--color-ink-muted)]"
          />
          <span className="text-[var(--color-ink-muted)]">
            {OPERATOR_LABEL.get(filter.operator) ?? filter.operator}
          </span>
          {filter.value !== undefined && filter.value !== '' ? (
            <span className="max-w-[180px] truncate font-medium text-[var(--color-ink)]">
              {filter.value}
            </span>
          ) : null}
          <button
            type="button"
            disabled={superseded}
            aria-label={`Remove filter on ${filter.column}`}
            onClick={() => onChange(filters.filter((_, position) => position !== index))}
            className="text-[var(--color-ink-faint)] transition-colors hover:text-[var(--color-ink)]"
          >
            <X className="size-3" />
          </button>
        </span>
      ))}

      {superseded ? (
        // Offering a filter that will not be applied is worse than offering none.
        <span className="shrink-0 text-[11px] text-[var(--color-ink-faint)]">
          {filters.length > 0 ? 'not applied — the statement defines the query' : null}
        </span>
      ) : (
        <>
          <AddFilter
            columns={columns}
            labels={labels}
            runId={runId}
            onAdd={(filter) => onChange([...filters, filter])}
          />

          {filters.length > 0 ? (
            <Button
              size="sm"
              variant="ghost"
              className="h-8 shrink-0 px-2 text-[11px]"
              onClick={() => onChange([])}
            >
              Clear filters
            </Button>
          ) : null}
        </>
      )}
    </>
  );
}

function AddFilter({
  columns,
  labels,
  runId,
  onAdd,
}: {
  columns: QueryColumn[];
  labels: Map<string, ColumnLabel>;
  runId: string | null;
  onAdd: (filter: BrowseFilter) => void;
}): ReactNode {
  return (
    <Popover
      width={360}
      trigger={({ toggle, open, id }) => (
        <Button
          size="sm"
          variant="ghost"
          onClick={toggle}
          aria-expanded={open}
          aria-controls={id}
          className="h-8 shrink-0 px-2 text-[12px]"
        >
          <Plus className="size-3.5" />
          Filter
        </Button>
      )}
    >
      {({ close }) => (
        <FilterBuilder
          columns={columns}
          labels={labels}
          runId={runId}
          onAdd={(filter) => {
            onAdd(filter);
            close();
          }}
          onCancel={close}
        />
      )}
    </Popover>
  );
}

/**
 * Building one filter: find the field, then say what it should be.
 *
 * A materialization can have hundreds of fields, so the first step is a search rather than a long
 * select: type any part of a path — `email`, `items sku` — and the best matches come first, with
 * the arrow keys and Enter to choose. Each time the builder opens it starts at the search.
 */
function FilterBuilder({
  columns,
  labels,
  runId,
  onAdd,
  onCancel,
}: {
  columns: QueryColumn[];
  labels: Map<string, ColumnLabel>;
  runId: string | null;
  onAdd: (filter: BrowseFilter) => void;
  onCancel: () => void;
}): ReactNode {
  const [column, setColumn] = useState<QueryColumn | null>(null);

  if (column === null) {
    return <FieldPicker columns={columns} onPick={setColumn} />;
  }
  return (
    <FilterValue
      column={column}
      labels={labels}
      runId={runId}
      onChangeField={() => setColumn(null)}
      onAdd={onAdd}
      onCancel={onCancel}
    />
  );
}

/** How many matches the picker lists at once; a longer list is a reason to type more. */
const PICKER_LIMIT = 200;

function FieldPicker({ columns, onPick }: { columns: QueryColumn[]; onPick: (column: QueryColumn) => void }): ReactNode {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const listId = useId();
  const listRef = useRef<HTMLUListElement>(null);

  // Fields first, then the Kafka metadata, each in the order the grid shows them.
  const ordered = useMemo(
    () => [...columns.filter((entry) => groupOf(entry.name) === 'payload'), ...columns.filter((entry) => groupOf(entry.name) === 'kafka')],
    [columns],
  );
  const deferredQuery = useDeferredValue(query);
  const matches = useMemo(() => searchFields(ordered, deferredQuery), [ordered, deferredQuery]);
  const shown = matches.slice(0, PICKER_LIMIT);
  const searching = deferredQuery.trim() !== '';
  const activeIndex = Math.min(active, Math.max(0, shown.length - 1));

  useEffect(() => {
    listRef.current?.querySelector(`[data-index="${activeIndex}"]`)?.scrollIntoView({ block: 'nearest' });
  }, [activeIndex]);

  const move = (delta: number): void => {
    if (shown.length === 0) return;
    setActive(Math.min(shown.length - 1, Math.max(0, activeIndex + delta)));
  };

  return (
    <div className="p-2">
      <div className="relative">
        <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-[var(--color-ink-faint)]" />
        <Input
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setActive(0);
          }}
          onKeyDown={(event) => {
            if (event.key === 'ArrowDown') move(1);
            else if (event.key === 'ArrowUp') move(-1);
            else if (event.key === 'PageDown') move(8);
            else if (event.key === 'PageUp') move(-8);
            else if (event.key === 'Enter') {
              const choice = shown[activeIndex];
              if (choice) onPick(choice.column);
            } else return;
            event.preventDefault();
          }}
          placeholder="Find a field"
          spellCheck={false}
          autoFocus
          role="combobox"
          aria-label="Find a field to filter on"
          aria-expanded="true"
          aria-controls={listId}
          aria-activedescendant={shown.length > 0 ? `${listId}-${activeIndex}` : undefined}
          className="numeric h-8 pl-8 text-[12px]"
        />
      </div>

      <ul id={listId} ref={listRef} role="listbox" aria-label="Fields" className="mt-1.5 max-h-[300px] overflow-y-auto">
        {shown.map((match, index) => {
          const groupStart = !searching && (index === 0 || groupOf(shown[index - 1]?.column.name ?? '') !== groupOf(match.column.name));
          return (
            <li key={match.column.name} role="presentation">
              {groupStart ? (
                <p className="px-2 pt-1.5 pb-1 text-[10px] font-semibold tracking-wide text-[var(--color-ink-faint)] uppercase">
                  {groupOf(match.column.name) === 'kafka' ? 'Kafka metadata' : 'Fields'}
                </p>
              ) : null}
              <button
                type="button"
                id={`${listId}-${index}`}
                data-index={index}
                role="option"
                aria-selected={index === activeIndex}
                tabIndex={-1}
                onMouseMove={() => {
                  if (index !== activeIndex) setActive(index);
                }}
                onClick={() => onPick(match.column)}
                className={cn(
                  'flex w-full items-baseline justify-between gap-3 rounded px-2 py-1 text-left',
                  index === activeIndex ? 'bg-[var(--color-raised)]' : null,
                )}
              >
                <MatchedName name={match.column.name} ranges={match.ranges} />
                <span className="shrink-0 text-[10px] tracking-wide text-[var(--color-ink-faint)] uppercase">
                  {compactType(match.column.type)}
                </span>
              </button>
            </li>
          );
        })}
      </ul>

      <p className="px-2 pt-1.5 text-[11px] text-[var(--color-ink-faint)]" aria-live="polite">
        {matches.length === 0
          ? searching
            ? `No column matches “${deferredQuery.trim()}”.`
            : 'No columns yet — run a query first.'
          : matches.length > shown.length
            ? `Showing ${shown.length} of ${matches.length} — keep typing to narrow.`
            : searching
              ? `${matches.length} ${matches.length === 1 ? 'column matches' : 'columns match'} · ↑↓ to move, Enter to choose`
              : `${matches.length} columns · type to search`}
      </p>
    </div>
  );
}

/** A name with the parts a search matched in bold; the leaf keeps its weight when nothing matched. */
function MatchedName({ name, ranges }: { name: string; ranges: Array<[number, number]> }): ReactNode {
  if (ranges.length === 0) {
    const cut = name.lastIndexOf('.');
    return (
      <span className="numeric min-w-0 truncate text-[12px]" title={name}>
        {cut === -1 ? null : <span className="text-[var(--color-ink-faint)]">{name.slice(0, cut + 1)}</span>}
        <span className="text-[var(--color-ink)]">{cut === -1 ? name : name.slice(cut + 1)}</span>
      </span>
    );
  }
  return (
    <span className="numeric min-w-0 truncate text-[12px] text-[var(--color-ink-muted)]" title={name}>
      {segmentName(name, ranges).map((segment, index) =>
        segment.match ? (
          <mark key={index} className="bg-transparent font-semibold text-[var(--color-ink)]">
            {segment.text}
          </mark>
        ) : (
          <span key={index}>{segment.text}</span>
        ),
      )}
    </span>
  );
}

function FilterValue({
  column,
  labels,
  runId,
  onChangeField,
  onAdd,
  onCancel,
}: {
  column: QueryColumn;
  labels: Map<string, ColumnLabel>;
  runId: string | null;
  onChangeField: () => void;
  onAdd: (filter: BrowseFilter) => void;
  onCancel: () => void;
}): ReactNode {
  const [operator, setOperator] = useState<FilterOperator>('eq');
  const [value, setValue] = useState('');

  const comparable = isComparableType(column.type);
  const operators = OPERATORS.filter((entry) => !entry.comparable || comparable);

  const suggestions = useQuery({
    queryKey: ['column-profile', runId, column.name],
    queryFn: () => api.runProfile(runId as string, column.name),
    enabled: runId !== null && !isUnary(operator),
    retry: 0,
    staleTime: Number.POSITIVE_INFINITY,
  });

  const submit = (overrideValue?: string): void => {
    const finalValue = overrideValue ?? value;
    if (!isUnary(operator) && finalValue.trim() === '') return;
    onAdd({ column: column.name, operator, value: isUnary(operator) ? undefined : finalValue });
  };

  return (
    <div className="space-y-2 p-2.5">
      <div className="flex items-center gap-1.5">
        <Filter className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />
        <ColumnLabelText labels={labels} name={column.name} className="numeric min-w-0 flex-1 text-[12px] font-medium" />
        <span className="shrink-0 text-[10px] tracking-wide text-[var(--color-ink-faint)] uppercase">
          {compactType(column.type)}
        </span>
        <button
          type="button"
          onClick={onChangeField}
          className="shrink-0 rounded px-1.5 py-0.5 text-[11px] text-[var(--color-ink-muted)] transition-colors hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]"
        >
          Change
        </button>
      </div>

      <div className="flex items-center gap-1.5">
        <Select
          value={operator}
          onChange={(event) => setOperator(event.target.value as FilterOperator)}
          aria-label="Filter operator"
          className="h-8 w-[116px] shrink-0 text-[12px]"
        >
          {operators.map((entry) => (
            <option key={entry.value} value={entry.value}>
              {entry.label}
            </option>
          ))}
        </Select>
        {isUnary(operator) ? null : (
          <Input
            value={value}
            onChange={(event) => setValue(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') submit();
            }}
            placeholder="Value"
            spellCheck={false}
            autoFocus
            aria-label="Filter value"
            className="numeric h-8 min-w-0 flex-1 text-[12px]"
          />
        )}
      </div>

      {isUnary(operator) ? null : (
        <div className="min-h-[24px]">
          {/*
            `isLoading`, not `isPending`. A disabled query reports `isPending: true` forever in
            TanStack v5, so keying the spinner on it would leave "Reading values…" spinning
            indefinitely whenever there is no run to read values from.
          */}
          {runId === null ? (
            <p className="px-1 text-[11px] text-[var(--color-ink-faint)]">Type a value.</p>
          ) : suggestions.isLoading ? (
            <p className="flex items-center gap-1.5 px-1 text-[11px] text-[var(--color-ink-muted)]">
              <Spinner className="size-3" />
              Reading values…
            </p>
          ) : suggestions.isError ? (
            <p className="px-1 text-[11px] text-[var(--color-phase-paused)]">Could not read values. Type one instead.</p>
          ) : suggestions.data && suggestions.data.topValues.length > 0 ? (
            <>
              <p className="px-1 pb-1 text-[10px] font-semibold tracking-wide text-[var(--color-ink-faint)] uppercase">
                Values in view
              </p>
              <ul className="max-h-[168px] space-y-0.5 overflow-y-auto">
                {suggestions.data.topValues.map((entry, index) => (
                  <li key={`${entry.value ?? 'null'}-${index}`}>
                    <button
                      type="button"
                      disabled={entry.value === null}
                      onClick={() => submit(entry.value ?? '')}
                      className="flex w-full items-baseline justify-between gap-2 rounded px-1.5 py-1 text-left transition-colors hover:bg-[var(--color-raised)] disabled:opacity-50"
                    >
                      <span className="numeric truncate text-[11px] text-[var(--color-ink)]">
                        {entry.value === null ? 'null' : entry.value}
                      </span>
                      <span className="numeric shrink-0 text-[11px] text-[var(--color-ink-faint)]">
                        {formatCount(entry.count)}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
              {suggestions.data.partial ? (
                <p className="px-1 pt-1 text-[10px] text-[var(--color-ink-faint)]">
                  Partial — the range was too wide to summarise in time.
                </p>
              ) : null}
            </>
          ) : (
            <p className="px-1 text-[11px] text-[var(--color-ink-faint)]">
              {suggestions.data?.partial
                ? 'Values unavailable for this range. Type one instead.'
                : 'No values in view. Type one instead.'}
            </p>
          )}
        </div>
      )}

      <div className="flex justify-end gap-1.5 border-t border-[var(--color-line)] pt-2">
        <Button size="sm" variant="ghost" onClick={onCancel}>
          Cancel
        </Button>
        <Button size="sm" variant="primary" onClick={() => submit()}>
          Add filter
        </Button>
      </div>
    </div>
  );
}
