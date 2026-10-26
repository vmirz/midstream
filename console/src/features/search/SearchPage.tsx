import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { Database, FolderOpen, Search, Table2 } from 'lucide-react';
import { useDeferredValue, useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';

import { PageBody, PageHeader } from '@/components/AppShell';
import { DomainChip } from '@/components/DomainChip';
import { EmptyState, Input, Panel, Skeleton, Spinner } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { cn } from '@/lib/cn';
import { useDomains } from '@/lib/domains';
import { rootQueryClient } from '@/lib/queryClient';
import { FOCUS_SEARCH_EVENT } from '@/lib/shortcuts';
import type { SearchResult } from '@shared/types';

import { resultPath } from './results';

const KINDS: Array<{ kind: SearchResult['kind']; title: string; icon: typeof Database }> = [
  { kind: 'materialization', title: 'Materializations', icon: Database },
  { kind: 'table', title: 'Tables', icon: Table2 },
  { kind: 'dataset', title: 'Datasets', icon: FolderOpen },
];

/**
 * Search every domain (handbook 20): materializations by name, id or topic, tables by name, datasets
 * by name or description, in each domain where the person may see them. Results open in their domain.
 */
export function SearchPage(): ReactNode {
  const [params, setParams] = useSearchParams();
  const [text, setText] = useState(() => params.get('q') ?? '');
  const query = useDeferredValue(text.trim());
  const domains = useDomains();
  const names = useMemo(() => new Map(domains.map((domain) => [domain.id, domain.name])), [domains]);
  const many = domains.length > 1;
  const input = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const [active, setActive] = useState(0);

  useEffect(() => {
    setParams(query ? { q: query } : {}, { replace: true });
    setActive(0);
  }, [query, setParams]);

  useEffect(() => {
    const focus = (): void => input.current?.focus();
    window.addEventListener(FOCUS_SEARCH_EVENT, focus);
    return () => window.removeEventListener(FOCUS_SEARCH_EVENT, focus);
  }, []);

  const search = useQuery(
    {
      queryKey: ['search', query],
      queryFn: () => api.search(query),
      enabled: query.length > 0,
      placeholderData: keepPreviousData,
      staleTime: 5_000,
    },
    rootQueryClient,
  );
  const results = query ? (search.data?.results ?? []) : [];
  // Keyboard order: the order the groups show them in.
  const ordered = useMemo(() => KINDS.flatMap(({ kind }) => results.filter((result) => result.kind === kind)), [results]);

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActive((index) => Math.min(index + 1, Math.max(ordered.length - 1, 0)));
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActive((index) => Math.max(index - 1, 0));
    } else if (event.key === 'Enter' && ordered[active]) {
      event.preventDefault();
      void navigate(resultPath(ordered[active]!));
    }
  };

  return (
    <>
      <PageHeader title="Search" subtitle={many ? 'Materializations, tables and datasets in every domain you may see.' : 'Materializations, tables and datasets.'}>
        <div className="mx-auto max-w-[1400px] px-6 pb-4">
          <div className="relative">
            <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
            <Input
              ref={input}
              autoFocus
              value={text}
              onChange={(event) => setText(event.target.value)}
              onKeyDown={onKeyDown}
              placeholder={many ? 'A name, an id, a topic, or domain.dataset.table' : 'A name, an id, a topic, or dataset.table'}
              className="pl-9"
              aria-label="Search"
              role="combobox"
              aria-expanded={ordered.length > 0}
              aria-controls="search-results"
              aria-activedescendant={ordered[active] ? `search-result-${active}` : undefined}
            />
            {search.isFetching ? <Spinner className="absolute top-1/2 right-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" /> : null}
          </div>
        </div>
      </PageHeader>

      <PageBody>
        {!query ? (
          <Panel>
            <EmptyState
              icon={<Search className="size-6" />}
              title="Find anything"
              description="Type part of a materialization's name or id, a topic, a table or a dataset. Press ⌘K or Ctrl+K anywhere to come back here."
            />
          </Panel>
        ) : search.isLoading ? (
          <Panel className="space-y-3 p-5">
            <Skeleton className="h-4 w-1/3" />
            <Skeleton className="h-4 w-2/3" />
          </Panel>
        ) : search.isError ? (
          <Panel>
            <EmptyState title="Search failed" description={search.error instanceof Error ? search.error.message : String(search.error)} />
          </Panel>
        ) : results.length === 0 ? (
          <Panel>
            <EmptyState icon={<Search className="size-6" />} title="Nothing matches" description="Try fewer letters, or another name." />
          </Panel>
        ) : (
          <div id="search-results" role="listbox" className="space-y-5">
            {KINDS.map(({ kind, title, icon: Icon }) => {
              const found = results.filter((result) => result.kind === kind);
              if (found.length === 0) return null;
              return (
                <section key={kind} aria-label={title} className="space-y-2">
                  <h2 className="px-1 text-[11px] font-medium tracking-wide text-[var(--color-ink-faint)] uppercase">{title}</h2>
                  <Panel>
                    <ul className="divide-y divide-[var(--color-line)]">
                      {found.map((result) => {
                        const index = ordered.indexOf(result);
                        return (
                          <li key={`${result.domain}:${result.kind}:${result.id}`}>
                            <Link
                              id={`search-result-${index}`}
                              role="option"
                              aria-selected={index === active}
                              to={resultPath(result)}
                              onMouseEnter={() => setActive(index)}
                              className={cn(
                                'flex items-center gap-3 px-5 py-3 transition-colors hover:bg-[var(--color-raised)]',
                                index === active && 'bg-[var(--color-raised)]',
                              )}
                            >
                              <Icon className="size-4 shrink-0 text-[var(--color-ink-faint)]" />
                              <span className="min-w-0 flex-1">
                                <span className={cn('block truncate text-[13px] font-medium', kind !== 'materialization' && 'numeric')}>{result.label}</span>
                                {result.detail ? (
                                  <span className="block truncate text-[12px] text-[var(--color-ink-muted)]">{result.detail}</span>
                                ) : null}
                              </span>
                              {many ? <DomainChip name={names.get(result.domain) ?? result.domain} id={result.domain} /> : null}
                            </Link>
                          </li>
                        );
                      })}
                    </ul>
                  </Panel>
                </section>
              );
            })}
            {search.data?.truncated ? (
              <p className="px-1 text-[12px] text-[var(--color-ink-muted)]">More matched than are shown: add letters to narrow it down.</p>
            ) : null}
          </div>
        )}
      </PageBody>
    </>
  );
}
