import { useMutation } from '@tanstack/react-query';
import { Check, ChevronsUpDown, Lock, Plus, Search } from 'lucide-react';
import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react';

import { Button, FIELD_BASE, Input } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useCan } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { matchName, searchTerms } from '@/lib/nameSearch';
import { useApi } from '@/lib/domains';
import { datasetNameProblem, type NameRules } from '@shared/tableNames';
import type { CpDataset } from '@shared/types';

import { useDatasets, useRefreshDatasets } from './useDatasets';

/**
 * The dataset a table goes in: a searchable list of every dataset, those the person cannot add
 * tables to shown locked, and - for someone who may create datasets - a new one made in place. A
 * dataset made here is its own request, applied before the materialization is submitted.
 */
export function DatasetPicker({
  value,
  onChange,
  rules,
  invalid = false,
  label,
}: {
  value: string;
  onChange: (dataset: string) => void;
  /** The rules a new dataset's name meets. */
  rules: NameRules;
  invalid?: boolean;
  /** What the field is for, for a screen reader: `Dataset for orders`. */
  label: string;
}): ReactNode {
  const listId = useId();
  const containerRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const datasets = useDatasets();
  const canCreate = useCan('datasets.create');

  const items = useMemo(() => {
    const terms = searchTerms(query);
    const all = datasets.data?.datasets ?? [];
    const found = terms.length === 0 ? all : all.filter((dataset) => matchName(`${dataset.name} ${dataset.description}`.toLowerCase(), terms) !== null);
    // Datasets the person may add tables to first, each group by name.
    return [...found].sort((a, b) => Number(!writable(a)) - Number(!writable(b)) || a.name.localeCompare(b.name));
  }, [datasets.data, query]);

  useEffect(() => {
    if (!open) return;
    searchRef.current?.focus();
    const onPointerDown = (event: PointerEvent): void => {
      if (!containerRef.current?.contains(event.target as Node)) close();
    };
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, [open]);

  useEffect(() => setActive(0), [query]);

  function close(): void {
    setOpen(false);
    setCreating(false);
    setQuery('');
  }

  function choose(dataset: CpDataset | undefined): void {
    if (!dataset || !writable(dataset)) return;
    onChange(dataset.name);
    close();
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>): void {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActive((current) => Math.min(current + 1, items.length - 1));
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActive((current) => Math.max(current - 1, 0));
    } else if (event.key === 'Enter') {
      // Never submits the dialog's form.
      event.preventDefault();
      choose(items[active]);
    } else if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      close();
    }
  }

  return (
    <div ref={containerRef} className="relative min-w-0 flex-1">
      <button
        type="button"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={label}
        onClick={() => (open ? close() : setOpen(true))}
        className={cn(
          FIELD_BASE,
          'flex h-8 cursor-pointer items-center gap-2 text-left text-[13px]',
          invalid && 'border-[var(--color-phase-failed)]',
          open && 'border-[var(--color-accent)]',
        )}
      >
        <span className={cn('numeric min-w-0 flex-1 truncate', !value && 'text-[var(--color-ink-faint)]')}>{value || 'Choose dataset'}</span>
        <ChevronsUpDown className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />
      </button>

      {open ? (
        <div
          className={cn(
            'fade-in absolute inset-x-0 top-[calc(100%+6px)] z-50 min-w-[260px] overflow-hidden',
            'rounded-[var(--radius-panel)] border border-[var(--color-line-strong)]',
            'bg-[var(--color-surface)] shadow-xl shadow-black/10 dark:shadow-black/50',
          )}
        >
          {creating ? (
            <NewDatasetForm
              rules={rules}
              initialName={query.trim().toLowerCase()}
              onCancel={() => setCreating(false)}
              onCreated={(name) => {
                onChange(name);
                close();
              }}
            />
          ) : (
            <>
              <div className="flex items-center gap-2 border-b border-[var(--color-line)] px-3">
                <Search className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />
                <input
                  ref={searchRef}
                  role="combobox"
                  aria-expanded
                  aria-controls={listId}
                  aria-activedescendant={items[active] ? `${listId}-${active}` : undefined}
                  aria-label="Search datasets"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  onKeyDown={onKeyDown}
                  placeholder="Search datasets"
                  spellCheck={false}
                  autoComplete="off"
                  className="h-9 min-w-0 flex-1 bg-transparent text-[13px] text-[var(--color-ink)] placeholder:text-[var(--color-ink-faint)] focus:outline-none"
                />
              </div>
              <div id={listId} role="listbox" aria-label="Datasets" className="max-h-56 overflow-y-auto overscroll-contain p-1">
                {datasets.isPending ? (
                  <p className="px-2 py-2 text-[12px] text-[var(--color-ink-muted)]">Loading datasets…</p>
                ) : datasets.isError ? (
                  <p className="px-2 py-2 text-[12px] text-[var(--color-phase-failed)]">
                    {datasets.error instanceof ApiError ? datasets.error.message : 'The datasets could not be loaded.'}
                  </p>
                ) : items.length === 0 ? (
                  <p className="px-2 py-2 text-[12px] text-[var(--color-ink-muted)]">
                    {query.trim() ? 'No datasets match.' : 'There are no datasets yet.'}
                  </p>
                ) : (
                  items.map((dataset, position) => {
                    const allowed = writable(dataset);
                    return (
                      <div
                        key={dataset.dataset_id}
                        id={`${listId}-${position}`}
                        role="option"
                        aria-selected={position === active}
                        aria-disabled={!allowed || undefined}
                        // Focus stays in the search box, which drives the list through aria-activedescendant.
                        tabIndex={-1}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            choose(dataset);
                          }
                        }}
                        title={allowed ? dataset.description || undefined : `You cannot add tables to ${dataset.name}: ask one of its owners for write access`}
                        onPointerMove={() => setActive(position)}
                        onPointerDown={(event) => event.preventDefault()}
                        onClick={() => choose(dataset)}
                        className={cn(
                          'flex items-center gap-2 rounded-md px-2 py-1.5 text-[13px]',
                          allowed ? 'cursor-pointer' : 'cursor-not-allowed opacity-60',
                          position === active && 'bg-[var(--color-accent-soft)]',
                        )}
                      >
                        {allowed ? (
                          <Check aria-hidden className={cn('size-3.5 shrink-0 text-[var(--color-accent)]', dataset.name !== value && 'invisible')} />
                        ) : (
                          <Lock aria-hidden className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />
                        )}
                        <span className="numeric shrink-0">{dataset.name}</span>
                        <span className="min-w-0 flex-1 truncate text-[12px] text-[var(--color-ink-faint)]">
                          {allowed ? dataset.description : 'no write access'}
                        </span>
                        <span className="numeric shrink-0 text-[11px] text-[var(--color-ink-faint)]" title="Tables in it">
                          {dataset.table_count}
                        </span>
                      </div>
                    );
                  })
                )}
              </div>
              {canCreate ? (
                <button
                  type="button"
                  onPointerDown={(event) => event.preventDefault()}
                  onClick={() => setCreating(true)}
                  className="flex w-full items-center gap-2 border-t border-[var(--color-line)] px-3 py-2 text-left text-[12px] font-medium text-[var(--color-accent)] hover:bg-[var(--color-raised)]"
                >
                  <Plus className="size-3.5" />
                  New dataset
                </button>
              ) : null}
            </>
          )}
        </div>
      ) : null}
    </div>
  );
}

/** Whether the person may put tables in a dataset, as the control plane told them. */
export function writable(dataset: Pick<CpDataset, 'permissions'>): boolean {
  return dataset.permissions.includes('dataset.write');
}

/**
 * Create a dataset in place: its name, fixed once made, and an optional description. Answers once the
 * directory has applied it, so the dataset can be chosen straight away.
 */
function NewDatasetForm({
  rules,
  initialName,
  onCancel,
  onCreated,
}: {
  rules: NameRules;
  initialName: string;
  onCancel: () => void;
  onCreated: (name: string) => void;
}): ReactNode {
  const api = useApi();
  const [name, setName] = useState(initialName);
  const [description, setDescription] = useState('');
  const refresh = useRefreshDatasets();
  const problem = datasetNameProblem(name, rules);
  const create = useMutation({
    mutationFn: () => api.createDataset({ name: name.trim(), ...(description.trim() ? { description: description.trim() } : {}) }),
    onSuccess: async (result) => {
      await refresh();
      onCreated(result.name ?? name.trim());
    },
  });
  const failure = create.error instanceof ApiError ? create.error.message : create.error ? 'The dataset could not be created.' : null;

  // A form inside the dialog's form: Enter creates the dataset, never the materialization.
  const submit = (): void => {
    if (!problem && !create.isPending) create.mutate();
  };

  return (
    <div className="space-y-2.5 p-3">
      <p className="text-[12px] font-medium">New dataset</p>
      <label className="block space-y-1">
        <span className="text-[11px] text-[var(--color-ink-muted)]">Name · fixed once created</span>
        <Input
          autoFocus
          value={name}
          onChange={(event) => setName(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              submit();
            } else if (event.key === 'Escape') {
              event.preventDefault();
              event.stopPropagation();
              onCancel();
            }
          }}
          placeholder="sales"
          spellCheck={false}
          autoComplete="off"
          className="numeric h-8 text-[13px]"
          aria-invalid={Boolean(name.trim() && problem)}
        />
        {name.trim() && problem ? <span className="block text-[11px] text-[var(--color-phase-failed)]">{problem}</span> : null}
      </label>
      <label className="block space-y-1">
        <span className="text-[11px] text-[var(--color-ink-muted)]">Description · optional</span>
        <Input
          value={description}
          onChange={(event) => setDescription(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              submit();
            }
          }}
          placeholder="Orders, refunds and the rest of sales"
          maxLength={500}
          className="h-8 text-[13px]"
        />
      </label>
      <p className="text-[11px] leading-relaxed text-[var(--color-ink-faint)]">
        You become its owner, and everyone signed in can read it; its owners can change that.
      </p>
      {failure ? <p className="text-[11px] text-[var(--color-phase-failed)]">{failure}</p> : null}
      <div className="flex justify-end gap-2">
        <Button type="button" size="sm" variant="ghost" onClick={onCancel}>
          Back
        </Button>
        <Button type="button" size="sm" variant="primary" disabled={Boolean(problem)} loading={create.isPending} onClick={submit}>
          Create
        </Button>
      </div>
    </div>
  );
}
