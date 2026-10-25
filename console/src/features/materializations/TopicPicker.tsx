import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Check, ChevronsUpDown, CornerDownLeft, RefreshCw, Search, TriangleAlert, X } from 'lucide-react';
import {
  useDeferredValue,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from 'react';

import { RelativeTime } from '@/components/Inline';
import { Badge, FIELD_BASE, Skeleton } from '@/components/ui/primitives';
import { ApiError, api } from '@/lib/api';
import { useCan } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { displayName, formatExact } from '@/lib/format';
import {
  isDeletedPhase,
  type MaterializationSummary,
  type TopicCatalogResponse,
  type TopicCategory,
  type TopicSummary,
} from '@shared/types';

import { indexTopics, searchTopics, segmentName, type TopicIndex, type TopicMatch } from './topicSearch';

const ROW_HEIGHT = 34;
const VISIBLE_ROWS = 8;
const OVERSCAN = 6;
/** Matches the server's TTL: reopening the dialog within it costs no request at all. */
const STALE_TIME_MS = 15_000;
const TOPICS_QUERY_KEY = ['topics'] as const;

const CATEGORY_LABELS: Record<TopicCategory, string> = {
  kafka: 'Kafka internal topic',
  materializer: 'used by the materializer itself',
  stream: 'state kept by a stream processor',
};
/** Characters kept at the end of a long name, where versions and suffixes tell topics apart. */
const TAIL_CHARS = 16;

/** A list entry: a matching topic, or the typed name used as-is. */
type Row = { kind: 'topic'; match: TopicMatch } | { kind: 'custom'; name: string };

/**
 * A topic name truncated in the middle, so the end stays readable: `orders.cdc.v2` and
 * `orders.cdc.v3` differ only there, and so do most topics that share a long prefix.
 */
function TopicName({
  name,
  ranges,
  emphasize,
  className,
}: {
  name: string;
  ranges: Array<[number, number]>;
  emphasize: boolean;
  className?: string;
}): ReactNode {
  const segments = emphasize ? segmentName(name, ranges) : [{ text: name, match: false }];
  const tailStart = Math.max(0, name.length - TAIL_CHARS);
  const head: typeof segments = [];
  const tail: typeof segments = [];
  let cursor = 0;
  for (const segment of segments) {
    const end = cursor + segment.text.length;
    if (end <= tailStart) head.push(segment);
    else if (cursor >= tailStart) tail.push(segment);
    else {
      head.push({ text: segment.text.slice(0, tailStart - cursor), match: segment.match });
      tail.push({ text: segment.text.slice(tailStart - cursor), match: segment.match });
    }
    cursor = end;
  }
  const render = (parts: typeof segments): ReactNode[] =>
    parts.map((segment, index) =>
      !emphasize ? (
        segment.text
      ) : segment.match ? (
        <mark key={index} className="bg-transparent font-semibold text-[var(--color-ink)]">
          {segment.text}
        </mark>
      ) : (
        <span key={index} className="text-[var(--color-ink-muted)]">
          {segment.text}
        </span>
      ),
    );
  return (
    <span className={cn('numeric flex min-w-0 flex-1', className)} title={name}>
      <span className="min-w-0 truncate">{render(head)}</span>
      <span className="shrink-0 whitespace-nowrap">{render(tail)}</span>
    </span>
  );
}

function partitionsLabel(count: number): string {
  return `${formatExact(count)} ${count === 1 ? 'partition' : 'partitions'}`;
}

/** Live materializations per source topic; a deleted one no longer reads its topic. */
function materializationsByTopic(
  items: readonly MaterializationSummary[],
): Map<string, MaterializationSummary[]> {
  const byTopic = new Map<string, MaterializationSummary[]>();
  for (const item of items) {
    if (isDeletedPhase(item)) continue;
    for (const topic of item.source_topics) {
      const existing = byTopic.get(topic);
      if (existing) existing.push(item);
      else byTopic.set(topic, [item]);
    }
  }
  return byTopic;
}

function buildRows(
  index: TopicIndex,
  names: ReadonlySet<string>,
  query: string,
  includeHidden: boolean,
): { rows: Row[]; hiddenMatches: number } {
  const result = searchTopics(index, query, { includeHidden });
  const rows: Row[] = result.matches.map((match) => ({ kind: 'topic', match }));
  const typed = query.trim();
  // Topic names are case-sensitive and a topic created moments ago may not be listed yet, so an
  // exact name that is not in the list can still be chosen, deliberately and visibly.
  if (typed && !names.has(typed)) rows.push({ kind: 'custom', name: typed });
  return { rows, hiddenMatches: result.hiddenMatches };
}

/**
 * Topics the busiest members of a runtime reading these topics can share between them: Faust gives
 * partition `i` of every topic with as many partitions to the same member, so topics of 32, 32 and
 * 8 partitions keep at most 40 members busy, not 72. Mirrors the control plane's `scale.py`.
 */
export function assignableMembers(partitionCounts: readonly number[]): number {
  return [...new Set(partitionCounts.filter((count) => count > 0))].reduce((sum, count) => sum + count, 0);
}

/**
 * Source topics field: a searchable list of every topic in the cluster, to choose one or more from.
 *
 * The list comes from `GET /api/topics`, which the server answers from one shared, pre-compressed
 * catalog, and is searched here in memory, so typing never waits on the network. Only the visible
 * rows are rendered, which keeps a cluster of tens of thousands of topics as responsive as one of
 * a dozen. Topics that are infrastructure rather than data are hidden until asked for, and topics
 * that are already materialized say so. A row toggles its topic and the list stays open, so several
 * can be picked in a row; each chosen topic shows, in the order chosen, the table it becomes - its
 * dataset and name, as the dialog renders them (`renderTable`).
 */
export function TopicPicker({
  label,
  required = false,
  value,
  onChange,
  maxTopics,
  materializations,
  renderTable,
  error,
  autoFocus = false,
  notes,
  verb = 'creating',
}: {
  label: string;
  required?: boolean;
  /** The chosen topics, in the order chosen: it orders the tables and settles a name two would share. */
  value: readonly string[];
  onChange: (topics: string[]) => void;
  /** The most topics one materialization reads. */
  maxTopics: number;
  materializations: readonly MaterializationSummary[];
  /** The table a chosen topic becomes, under its topic: the dataset and name, chosen or fixed. */
  renderTable: (topic: string, position: number) => ReactNode;
  error?: string | null;
  autoFocus?: boolean;
  /** A word on a chosen topic, such as that its table is new. */
  notes?: ReadonlyMap<string, string>;
  /** What saving the topics does, for the hints: `creating` or `saving`. */
  verb?: string;
}): ReactNode {
  const triggerId = useId();
  const listId = useId();
  const queryClient = useQueryClient();
  const containerRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [showHidden, setShowHidden] = useState(false);
  const [active, setActive] = useState(0);
  const [scrollTop, setScrollTop] = useState(0);
  const deferredQuery = useDeferredValue(query);

  // Without the permission to browse topics, a name is typed; nothing is fetched that would be refused.
  const canBrowse = useCan('topics.view');
  const catalog = useQuery({
    queryKey: TOPICS_QUERY_KEY,
    queryFn: () => api.topics(),
    staleTime: STALE_TIME_MS,
    enabled: canBrowse,
  });
  const reload = useMutation({
    mutationFn: () => api.topics(true),
    onSuccess: (data) => queryClient.setQueryData<TopicCatalogResponse>(TOPICS_QUERY_KEY, data),
  });

  const index = useMemo(() => indexTopics(catalog.data?.topics ?? []), [catalog.data]);
  const byName = useMemo(
    () => new Map<string, TopicSummary>(index.topics.map((topic) => [topic.name, topic])),
    [index],
  );
  const names = useMemo(() => new Set(byName.keys()), [byName]);
  const usage = useMemo(() => materializationsByTopic(materializations), [materializations]);
  const { rows, hiddenMatches } = useMemo(
    () => buildRows(index, names, deferredQuery, showHidden),
    [index, names, deferredQuery, showHidden],
  );

  const chosen = useMemo(() => new Set(value), [value]);
  const full = value.length >= maxTopics;
  const searching = deferredQuery.trim().length > 0;
  const topicMatches = rows.length - (rows[rows.length - 1]?.kind === 'custom' ? 1 : 0);

  useEffect(() => {
    setActive(0);
  }, [deferredQuery, showHidden]);

  useEffect(() => {
    if (!open) return;
    searchRef.current?.focus();
    const onPointerDown = (event: PointerEvent): void => {
      if (!containerRef.current?.contains(event.target as Node)) closePicker(false);
    };
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, [open]);

  // Keep the active row in view as the keyboard moves it. The rendered window follows in this same
  // pass rather than on the browser's later scroll event, so the row `aria-activedescendant` names
  // always exists, even mid key-repeat.
  useLayoutEffect(() => {
    const list = listRef.current;
    if (!open || !list) return;
    const top = active * ROW_HEIGHT;
    if (top < list.scrollTop) list.scrollTop = top;
    else if (top + ROW_HEIGHT > list.scrollTop + list.clientHeight) {
      list.scrollTop = top + ROW_HEIGHT - list.clientHeight;
    }
    setScrollTop(list.scrollTop);
  }, [active, open, rows]);

  function openPicker(initialQuery = ''): void {
    setQuery(initialQuery);
    setOpen(true);
    if (!initialQuery) {
      const position = rows.findIndex((row) => row.kind === 'topic' && row.match.topic.name === value[value.length - 1]);
      setActive(Math.max(0, position));
    }
  }

  function closePicker(restoreFocus: boolean): void {
    setOpen(false);
    setQuery('');
    setScrollTop(0);
    if (restoreFocus) triggerRef.current?.focus();
  }

  /** Add the row's topic, or take it out again; the list stays open for the next. */
  function choose(row: Row | undefined): void {
    if (!row) return;
    const topic = row.kind === 'topic' ? row.match.topic.name : row.name;
    if (chosen.has(topic)) {
      onChange(value.filter((name) => name !== topic));
      return;
    }
    if (full) return;
    onChange([...value, topic]);
    // A search finds one topic; the next is searched for afresh.
    if (query) {
      setQuery('');
      setScrollTop(0);
    }
    searchRef.current?.focus();
  }

  function remove(topic: string): void {
    onChange(value.filter((name) => name !== topic));
  }

  function onSearchKeyDown(event: KeyboardEvent<HTMLInputElement>): void {
    const last = rows.length - 1;
    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        setActive((current) => Math.min(current + 1, last));
        break;
      case 'ArrowUp':
        event.preventDefault();
        setActive((current) => Math.max(current - 1, 0));
        break;
      case 'PageDown':
        event.preventDefault();
        setActive((current) => Math.min(current + VISIBLE_ROWS, last));
        break;
      case 'PageUp':
        event.preventDefault();
        setActive((current) => Math.max(current - VISIBLE_ROWS, 0));
        break;
      case 'Enter': {
        // Enter must never submit the dialog's form, and must act on what is typed now even if
        // the deferred list has not caught up with the last keystroke.
        event.preventDefault();
        const current = query === deferredQuery ? rows[active] : buildRows(index, names, query, showHidden).rows[0];
        choose(current);
        break;
      }
      case 'Escape':
        // The dialog closes on Escape at the document; this keypress only closes the list.
        event.preventDefault();
        event.stopPropagation();
        closePicker(true);
        break;
      case 'Backspace':
        // On an empty search, takes the last topic chosen back out, as a token field does.
        if (query === '' && value.length > 0) {
          event.preventDefault();
          onChange(value.slice(0, -1));
        }
        break;
      case 'Tab':
        closePicker(false);
        break;
      default:
        break;
    }
  }

  function onTriggerKeyDown(event: KeyboardEvent<HTMLButtonElement>): void {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      openPicker();
    } else if (event.key.length === 1 && event.key !== ' ' && !event.metaKey && !event.ctrlKey && !event.altKey) {
      // Type-ahead: typing on the closed field starts the search with that character.
      event.preventDefault();
      openPicker(event.key);
    }
  }

  const loadError = !canBrowse
    ? 'browsing topics needs the “Browse Kafka topics” permission'
    : catalog.isError && !catalog.data
      ? catalog.error instanceof ApiError
        ? String(catalog.error.details.reason ?? catalog.error.message)
        : 'the topic list could not be loaded'
      : null;

  const first = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const last = Math.min(rows.length, first + VISIBLE_ROWS + OVERSCAN * 2);

  const summary = !canBrowse
    ? 'Topic list unavailable'
    : catalog.isPending
    ? 'Loading topics…'
    : searching
      ? `${formatExact(topicMatches)} ${topicMatches === 1 ? 'match' : 'matches'}`
      : `${formatExact(topicMatches)} ${topicMatches === 1 ? 'topic' : 'topics'}`;

  function renderRow(row: Row, position: number): ReactNode {
    const isActive = position === active;
    const topicName = row.kind === 'topic' ? row.match.topic.name : row.name;
    const isChosen = chosen.has(topicName);
    const blocked = full && !isChosen;
    const shared = {
      id: `${listId}-${position}`,
      role: 'option' as const,
      'aria-selected': isActive,
      onPointerMove: () => {
        if (!isActive) setActive(position);
      },
      // Keep focus, and the caret, in the search box while clicking a row.
      onPointerDown: (event: ReactPointerEvent) => event.preventDefault(),
      onClick: () => choose(row),
      style: { top: position * ROW_HEIGHT, height: ROW_HEIGHT },
      'aria-disabled': blocked || undefined,
      title: blocked ? `A materialization reads at most ${maxTopics} topics` : undefined,
      className: cn(
        'absolute inset-x-0 flex items-center gap-2 rounded-md px-2.5 text-[13px]',
        blocked ? 'cursor-not-allowed opacity-50' : 'cursor-pointer',
        isActive && 'bg-[var(--color-accent-soft)]',
      ),
    };

    if (row.kind === 'custom') {
      return (
        <div key="custom" {...shared}>
          <CornerDownLeft className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />
          <span className="min-w-0 flex-1 truncate text-[var(--color-ink-muted)]">
            {isChosen ? 'Remove' : 'Use'} <span className="numeric font-medium text-[var(--color-ink)]">{row.name}</span>
          </span>
          <span className="shrink-0 text-[11px] text-[var(--color-ink-faint)]">not in the list</span>
        </div>
      );
    }

    const { topic, ranges } = row.match;
    const uses = usage.get(topic.name) ?? [];
    return (
      <div key={topic.name} {...shared}>
        <TopicName
          name={topic.name}
          ranges={ranges}
          emphasize={searching}
          className={topic.category ? 'text-[var(--color-ink-muted)]' : 'text-[var(--color-ink)]'}
        />
        {topic.category ? (
          <Badge className="shrink-0" title={`Hidden by default: ${CATEGORY_LABELS[topic.category]}`}>
            Internal
          </Badge>
        ) : null}
        {uses.length > 0 ? (
          <Badge className="shrink-0" title={`Materialized by ${uses.map(displayName).join(', ')}`}>
            {uses.length === 1 ? 'Materialized' : `Materialized ×${uses.length}`}
          </Badge>
        ) : null}
        <span
          className="numeric w-10 shrink-0 text-right text-[12px] text-[var(--color-ink-faint)]"
          title={`${partitionsLabel(topic.partitions)} · replication factor ${topic.replicationFactor}`}
        >
          {formatExact(topic.partitions)}
        </span>
        <Check aria-hidden className={cn('size-3.5 shrink-0 text-[var(--color-accent)]', !isChosen && 'invisible')} />
      </div>
    );
  }

  // Internal topics that one click would reveal: those matching the search, or all of them.
  const revealable = showHidden ? 0 : searching ? hiddenMatches : index.hiddenTotal;
  // Said whenever no topic matches, even when the "use this name" row is offered below it; a
  // failed load already says so above the list, and "no topics" would contradict it.
  const noTopics =
    !catalog.isPending && !loadError && topicMatches === 0 ? (
      <div className={cn('px-3 text-center text-[13px] text-[var(--color-ink-muted)]', rows.length ? 'py-3' : 'py-6')}>
        {searching ? 'No topics match.' : 'No data topics in this cluster.'}
        {revealable > 0 ? (
          <button
            type="button"
            onPointerDown={(event) => event.preventDefault()}
            onClick={() => setShowHidden(true)}
            className="mt-1 block w-full text-[12px] text-[var(--color-accent)] hover:underline"
          >
            Show {formatExact(revealable)} internal {revealable === 1 ? 'topic' : 'topics'}
          </button>
        ) : null}
      </div>
    ) : null;

  let listBody: ReactNode;
  if (catalog.isPending) {
    listBody = (
      <div className="space-y-2.5 px-3 py-3">
        {['w-[72%]', 'w-[56%]', 'w-[64%]', 'w-[48%]'].map((width) => (
          <Skeleton key={width} className={cn('h-3.5', width)} />
        ))}
      </div>
    );
  } else {
    listBody = (
      <>
        {noTopics}
        {rows.length > 0 ? (
          <div
            ref={listRef}
            id={listId}
            role="listbox"
            aria-label="Topics"
            onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
            className="overflow-y-auto overscroll-contain px-1"
            style={{ maxHeight: VISIBLE_ROWS * ROW_HEIGHT }}
          >
            <div className="relative" style={{ height: rows.length * ROW_HEIGHT }}>
              {rows.slice(first, last).map((row, offset) => renderRow(row, first + offset))}
            </div>
          </div>
        ) : null}
      </>
    );
  }

  const known = value.map((topic) => byName.get(topic)).filter((topic): topic is TopicSummary => topic !== undefined);
  const unknown = catalog.data ? value.filter((topic) => !byName.has(topic)) : [];
  const partitions = known.map((topic) => topic.partitions);
  const totalPartitions = partitions.reduce((sum, count) => sum + count, 0);

  let hint: ReactNode;
  if (error) {
    hint = <span className="block text-[12px] text-[var(--color-phase-failed)]">{error}</span>;
  } else if (unknown.length > 0) {
    hint = (
      <span className="flex items-center gap-1.5 text-[12px] text-[var(--color-phase-paused)]">
        <TriangleAlert className="size-3.5 shrink-0" />
        {unknown.length === 1 ? `${unknown[0]} is` : `${unknown.join(', ')} are`} not in the cluster's topic list. Check
        the {unknown.length === 1 ? 'name' : 'names'} before {verb}.
      </span>
    );
  } else if (value.length === 1 && known[0]) {
    const topic = known[0];
    hint = (
      <span className="block text-[12px] text-[var(--color-ink-muted)]">
        {partitionsLabel(topic.partitions)} · replication factor {topic.replicationFactor}
        {topic.category ? ` · ${CATEGORY_LABELS[topic.category]}` : ''}. Add more topics to read them with the same runtime.
      </span>
    );
  } else if (value.length > 1) {
    hint = (
      <span className="block text-[12px] text-[var(--color-ink-muted)]">
        {formatExact(value.length)} topics, one runtime: {partitionsLabel(totalPartitions)} in all
        {known.length === value.length ? `, which keep at most ${formatExact(assignableMembers(partitions))} runtime members busy` : ''}.
        {full ? ` A materialization reads at most ${formatExact(maxTopics)} topics.` : ''}
      </span>
    );
  } else if (loadError) {
    hint = (
      <span className="block text-[12px] text-[var(--color-ink-muted)]">
        The topic list is unavailable, so names can't be checked against the cluster.
      </span>
    );
  } else {
    hint = (
      <span className="block text-[12px] text-[var(--color-ink-muted)]">
        Search the cluster's topics, or type an exact topic name. Choose several to read them with one runtime, each
        into a table of its own.
      </span>
    );
  }

  return (
    <div className="space-y-1.5">
      <label htmlFor={triggerId} className="flex items-baseline gap-1.5">
        <span className="text-[13px] font-medium">{label}</span>
        {required ? <span className="text-[11px] text-[var(--color-ink-faint)]">required</span> : null}
      </label>

      <div ref={containerRef} className="relative">
        <button
          ref={triggerRef}
          id={triggerId}
          type="button"
          autoFocus={autoFocus}
          aria-haspopup="listbox"
          aria-expanded={open}
          onClick={() => (open ? closePicker(false) : openPicker())}
          onKeyDown={onTriggerKeyDown}
          className={cn(
            FIELD_BASE,
            'flex h-9 cursor-pointer items-center gap-2 text-left',
            error && 'border-[var(--color-phase-failed)]',
            open && 'border-[var(--color-accent)]',
          )}
        >
          {value.length === 0 ? (
            <span className="min-w-0 flex-1 truncate text-[var(--color-ink-faint)]">Select topics</span>
          ) : value.length === 1 ? (
            <TopicName name={value[0] as string} ranges={[]} emphasize={false} className="text-[13px]" />
          ) : (
            <span className="min-w-0 flex-1 truncate text-[13px]">
              {formatExact(value.length)} topics
              <span className="text-[var(--color-ink-faint)]"> · add or remove</span>
            </span>
          )}
          <ChevronsUpDown className="size-4 shrink-0 text-[var(--color-ink-faint)]" />
        </button>

        {open ? (
          <div
            className={cn(
              'fade-in absolute inset-x-0 top-[calc(100%+6px)] z-40 overflow-hidden',
              'rounded-[var(--radius-panel)] border border-[var(--color-line-strong)]',
              'bg-[var(--color-surface)] shadow-xl shadow-black/10 dark:shadow-black/50',
            )}
          >
            <div className="flex items-center gap-2 border-b border-[var(--color-line)] pr-1.5 pl-3">
              <Search className="size-4 shrink-0 text-[var(--color-ink-faint)]" />
              <input
                ref={searchRef}
                role="combobox"
                aria-expanded
                aria-controls={listId}
                aria-autocomplete="list"
                aria-activedescendant={rows[active] ? `${listId}-${active}` : undefined}
                aria-label="Search topics"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={onSearchKeyDown}
                placeholder="Search topics"
                spellCheck={false}
                autoComplete="off"
                className="h-10 min-w-0 flex-1 bg-transparent text-[13px] text-[var(--color-ink)] placeholder:text-[var(--color-ink-faint)] focus:outline-none"
              />
              <button
                type="button"
                onClick={() => reload.mutate()}
                disabled={reload.isPending}
                title="Reload the topic list from Kafka"
                aria-label="Reload the topic list from Kafka"
                className="grid size-7 shrink-0 place-items-center rounded-md text-[var(--color-ink-faint)] transition-colors hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)] disabled:opacity-50"
              >
                <RefreshCw className={cn('size-3.5', reload.isPending && 'animate-spin')} />
              </button>
            </div>

            <div className="flex items-center justify-between gap-3 px-3 pt-2 pb-1.5 text-[11px] text-[var(--color-ink-faint)]">
              <span className="tabular-nums">
                {summary}
                {searching && hiddenMatches > 0 && !showHidden ? ` · ${formatExact(hiddenMatches)} internal hidden` : ''}
              </span>
              {/* Right edge lines up with the partitions column: list padding + check icon + gap. */}
              {topicMatches > 0 ? <span className="mr-6">Partitions</span> : null}
            </div>

            {loadError ? (
              <p className="px-3 pb-2 text-[12px] text-[var(--color-phase-failed)]">
                Couldn't load topics: {loadError}. Type the exact topic name instead.
              </p>
            ) : null}

            {listBody}

            {reload.isError || catalog.data?.stale ? (
              <p
                className={cn(
                  'border-t border-[var(--color-line)] px-3 py-1.5 text-[11px]',
                  reload.isError ? 'text-[var(--color-phase-failed)]' : 'text-[var(--color-phase-paused)]',
                )}
                title={catalog.data?.error ?? undefined}
              >
                {reload.isError
                  ? 'Reload failed. Showing the last list.'
                  : 'Kafka is unreachable. Showing an earlier list.'}
              </p>
            ) : null}

            <div className="flex items-center justify-between gap-3 border-t border-[var(--color-line)] px-3 py-1.5 text-[11px] text-[var(--color-ink-faint)]">
              {index.hiddenTotal > 0 ? (
                <label className="flex cursor-pointer items-center gap-1.5 select-none">
                  <input
                    type="checkbox"
                    checked={showHidden}
                    onChange={(event) => {
                      setShowHidden(event.target.checked);
                      searchRef.current?.focus();
                    }}
                    className="size-3 cursor-pointer accent-[var(--color-accent)]"
                  />
                  Show internal topics ({formatExact(index.hiddenTotal)})
                </label>
              ) : (
                <span />
              )}
              {catalog.data ? <RelativeTime value={catalog.data.fetchedAt} prefix="Updated" /> : null}
            </div>
          </div>
        ) : null}
      </div>

      {value.length > 0 ? (
        <ul className="space-y-1" aria-label="Chosen topics and their tables">
          {value.map((topic, position) => {
            const uses = usage.get(topic) ?? [];
            return (
              <li
                key={topic}
                className="space-y-1 rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] py-1.5 pr-1 pl-2.5 text-[12px]"
              >
                <div className="flex items-center gap-2">
                  <TopicName name={topic} ranges={[]} emphasize={false} className="text-[var(--color-ink)]" />
                  {notes?.get(topic) ? <Badge className="shrink-0">{notes.get(topic)}</Badge> : null}
                  {uses.length > 0 ? (
                    <Badge className="shrink-0" title={`Also materialized by ${uses.map(displayName).join(', ')}; each materialization reads it independently`}>
                      {uses.length === 1 ? 'Materialized' : `Materialized ×${uses.length}`}
                    </Badge>
                  ) : null}
                  <button
                    type="button"
                    onClick={() => remove(topic)}
                    aria-label={`Remove ${topic}`}
                    title={`Remove ${topic}`}
                    className="grid size-6 shrink-0 place-items-center rounded-md text-[var(--color-ink-faint)] transition-colors hover:bg-[var(--color-surface)] hover:text-[var(--color-ink)]"
                  >
                    <X className="size-3.5" />
                  </button>
                </div>
                <div className="pr-1.5">{renderTable(topic, position)}</div>
              </li>
            );
          })}
        </ul>
      ) : null}

      {hint}
    </div>
  );
}
