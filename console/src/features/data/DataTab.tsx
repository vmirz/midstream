import { useQuery } from '@tanstack/react-query';
import { Code2, PanelLeftClose, PanelLeftOpen, Play, RefreshCw, Square, Table2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';

import { ColumnRail } from './ColumnRail';
import { FilterChips } from './FilterChips';
import { HeadPeek } from './HeadPeek';
import { autoRerunDelay, liveHold, liveTailAvailable, peekDone, tableQueryable, tailNotice, type TailNotice } from './live';
import { Pager } from './Pager';
import { ResizeHandle } from './ResizeHandle';
import { RowDrawer } from './RowDrawer';
import { SqlPanel } from './SqlPanel';
import { MoreActions } from './MoreActions';
import { StatusBar } from './StatusBar';
import { TimeRangePicker } from './TimeRangePicker';
import {
  buildColumnLabels,
  defaultVisibleColumns,
  gridOrder,
  groupOf,
  isListType,
  labelFor,
  resolveColumnName,
  type ColumnGroupId,
} from './columns';
import { ALL_TIME, describeInstants, rangeLabel, resolveRange, sameRange, type TimeRangeSpec } from './timeRange';
import { useRun, useRunPage } from './useRun';
import { readViewState, writeViewState } from './viewState';
import { resolveTable } from '../tables/selectedTable';
import { TablePicker } from '../tables/TablePicker';
import { PageBody } from '@/components/AppShell';
import { DataGrid } from '@/components/DataGrid';
import { useToast } from '@/components/ui/Toast';
import { Button, EmptyState, Panel, Select, Skeleton } from '@/components/ui/primitives';
import { ApiError, api as consoleApi } from '@/lib/api';
import { useAuth, useConsoleAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { formatTimestamp } from '@/lib/format';
import { useApi, useConsoleConfig, useDomain } from '@/lib/domains';
import { rootQueryClient } from '@/lib/queryClient';
import type {
  BrowseFilter,
  CpTableSpec,
  DetailSnapshot,
  QueryColumn,
  RunOrder,
  RunRequest,
  StatementPosition,
} from '@shared/types';
import { readableTables } from '@shared/types';

const SQL_MIN_HEIGHT = 120;
const SQL_MAX_HEIGHT = 640;
const SQL_HEIGHT_KEY = 'console.data.sqlHeight';
const RAIL_MIN_WIDTH = 264;
const RAIL_MAX_WIDTH = 720;
const RAIL_WIDTH_KEY = 'console.data.railWidth';
const RUN_HINT = typeof navigator !== 'undefined' && navigator.platform.toLowerCase().includes('mac') ? '⌘↵' : 'Ctrl+↵';

function stored(key: string, min: number, max: number, fallback: number): number {
  try {
    const raw = Number(localStorage.getItem(key));
    if (Number.isFinite(raw) && raw > 0) return Math.min(max, Math.max(min, raw));
  } catch {
    // A browser that blocks storage still gets working panes, just not remembered ones.
  }
  return fallback;
}

function remember(key: string, value: number): void {
  try {
    localStorage.setItem(key, String(Math.round(value)));
  } catch {
    /* not remembering a pane size is not worth failing a render over */
  }
}

/**
 * The question the controls describe, keyed so it can be compared with the one on screen.
 *
 * The range is keyed by its label, not its instants: "Last 15 minutes" pressed twice is the same
 * question asked again, which is what Run means.
 */
function viewKey(view: { table: string; sql: string | null; range: string; filters: BrowseFilter[]; order: RunOrder }): string {
  return view.sql !== null
    ? JSON.stringify({ kind: 'sql', table: view.table, range: view.range, sql: view.sql.trim() })
    : JSON.stringify({ kind: 'browse', table: view.table, range: view.range, filters: view.filters, order: view.order });
}

function requestKey(request: RunRequest): string {
  return viewKey({
    table: request.table ?? '',
    sql: request.kind === 'sql' ? (request.sql ?? '') : null,
    range: request.rangeLabel ?? '',
    filters: request.filters ?? [],
    order: request.order ?? 'newest',
  });
}

/**
 * Data exploration for one materialization.
 *
 * One way to ask a question: choose a table, set a time range, add filters or write SQL, and press
 * Run. Opening the tab runs the table's preview — every row, newest first, with its exact count —
 * which the table's manifests answer without reading anything, so it is on screen at once at any
 * size. Paging reads one data file's rows directly and the next page is fetched ahead, so turning
 * pages does not wait on a query. Every run reports its progress while it works and its exact row
 * count when it is done, and only then can its result be exported. A statement can read every table
 * of the materialization, joining them by name. Every run reads each table as it is now: snapshots
 * are not this tab's business.
 *
 * With live tail on (every materialization has it), every run also holds the records past its
 * snapshot, from the live tail, and the browse view's Live mode — on unless turned off — appends what arrives
 * every few seconds while the tab is seen; a commit replaces the run with one pinned to the new
 * snapshot. A statement gets its tail once, when it runs, and Auto re-run runs it again.
 */
export function DataTab({
  materializationId,
  snapshot,
}: {
  materializationId: string;
  snapshot: DetailSnapshot;
}): ReactNode {
  const api = useApi();
  const domain = useDomain();
  const toast = useToast();
  const [searchParams, setSearchParams] = useSearchParams();
  // The address bar seeds the view once; from then on the view drives the address bar.
  const initial = useRef(readViewState(searchParams)).current;

  // Kept tables too: their topics were removed, but they stay readable in their datasets.
  const materialization = snapshot.detail?.materialization;
  const tables = useMemo(() => (materialization ? readableTables(materialization) : []), [materialization]);
  // Only the tables whose datasets the person may query can be browsed or named in a statement.
  const { can } = useAuth();
  const consoleAuth = useConsoleAuth();
  const mayQuery = useCallback((table: CpTableSpec) => can('data.query', table.dataset_id), [can]);
  const queryable = useMemo(() => tables.filter(mayQuery), [tables, mayQuery]);
  // Each table's dataset, which SQL names it in: `<domain>.<dataset>.<table>`.
  const datasets = useMemo(() => new Map(tables.map((table) => [table.name, table.dataset])), [tables]);
  // Every other table a statement could name, in this domain and the others (handbook 20): offered
  // by name as the editor completes; each domain decides at run time whether the person may query it.
  const fleetQuery = useQuery({ queryKey: ['fleet-tables'], queryFn: () => consoleApi.fleet(), staleTime: 60_000 }, rootQueryClient);
  const otherTables = useMemo(
    () =>
      (fleetQuery.data?.domains ?? []).flatMap((entry) =>
        (entry.snapshot?.items ?? [])
          .filter((item) => !(entry.domain === domain.id && item.materialization_id === materializationId) && item.desired_phase !== 'deleted')
          .flatMap((item) =>
            item.tables.map((qualified) => {
              const dot = qualified.indexOf('.');
              return { domain: entry.domain, dataset: qualified.slice(0, dot), name: qualified.slice(dot + 1) };
            }),
          ),
      ),
    [fleetQuery.data, domain.id, materializationId],
  );
  const [requestedTable, setRequestedTable] = useState<string | null>(initial.table);
  // A table the address bar names that this materialization does not have, or that the person may
  // not query, shows the first they may instead.
  const tableName = resolveTable(tables, requestedTable, mayQuery)?.name ?? '';
  const [range, setRange] = useState<TimeRangeSpec>(initial.range);
  const [filters, setFilters] = useState<BrowseFilter[]>(initial.filters);
  const [order, setOrder] = useState<RunOrder>(initial.order);
  const [sqlMode, setSqlMode] = useState(initial.sql !== null);
  const [sqlDraft, setSqlDraft] = useState(initial.sql ?? '');
  const [page, setPage] = useState(initial.page);
  const [pageSize, setPageSize] = useState(initial.pageSize);
  const [visibleOverride, setVisibleOverride] = useState<string[] | null>(initial.visibleColumns);
  const [pinnedColumn, setPinnedColumn] = useState<string | null>(initial.pinnedColumn);
  const [railOpen, setRailOpen] = useState(true);
  const [railWidth, setRailWidth] = useState(() => stored(RAIL_WIDTH_KEY, RAIL_MIN_WIDTH, RAIL_MAX_WIDTH, RAIL_MIN_WIDTH));
  const [sqlHeight, setSqlHeight] = useState(() => stored(SQL_HEIGHT_KEY, SQL_MIN_HEIGHT, SQL_MAX_HEIGHT, 220));
  const [detailIndex, setDetailIndex] = useState<number | null>(null);
  const [validation, setValidation] = useState<{ sql: string; message: string; position: StatementPosition | null } | null>(null);
  const [live, setLive] = useState(initial.live);
  const [autoRerun, setAutoRerun] = useState(false);
  // A refresh refused for the person's session: Live stops until a new run, or a new sign-in.
  const [liveRefused, setLiveRefused] = useState(false);
  // Bumped after every Live look, success or not, so the next one is scheduled.
  const [liveTick, setLiveTick] = useState(0);
  const [peekOpen, setPeekOpen] = useState(false);
  const pageVisible = useDocumentVisible();
  // The range of the run on screen as chosen, so a run that replaces it slides with "Last 15 minutes".
  const runRange = useRef<TimeRangeSpec>(initial.range);

  const controller = useRun(materializationId);
  const { run, start, cancel, attach, starting, startError, refresh } = controller;
  const running = starting || run?.state === 'running';

  const summaryQuery = useQuery({
    queryKey: ['data-summary', materializationId, tableName],
    queryFn: () => api.dataSummary(materializationId, tableName),
    enabled: tableName !== '',
    refetchInterval: (query) => (query.state.error ? false : 30_000),
    retry: 0,
  });
  // A run Live started when a commit landed reads the table's newest snapshot: the summary, read
  // every 30 s, catches up with it.
  const runSnapshotId =
    run?.tables.find((table) => table.domain === domain.id && table.materializationId === materializationId && table.name === tableName)?.snapshotId ?? null;
  useEffect(() => {
    if (runSnapshotId === null || !summaryQuery.data) return;
    if (summaryQuery.data.snapshotId !== runSnapshotId) void summaryQuery.refetch();
    // Once for each snapshot a run reads, not for every answer of the summary.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runSnapshotId]);
  // The console config is read again each minute: live tail can be switched off, and on, for every
  // materialization of the domain. The domain's own part of it says how its tail is.
  const configQuery = useConsoleConfig();
  const domainTail = domain.liveTail;
  const summary = summaryQuery.data ?? null;
  const liveTail = liveTailAvailable(domainTail.enabled, materialization);
  // A table created but with nothing committed yet is queryable through its tail.
  const created = snapshot.detail?.tables.some((table) => table.name === tableName && table.state === 'active') ?? false;
  const queryableNow = tableQueryable({ available: summary?.available, liveTail, created });

  const schemaFields = snapshot.schemas[tableName]?.fields;
  const schemaOrder = useMemo(
    () => [...new Set((schemaFields ?? []).filter((field) => field.kind !== 'metadata').map((field) => field.name))],
    [schemaFields],
  );
  // Every table's columns as the Schema tab names them, for the editor to complete.
  const tableColumns = useMemo(
    () =>
      Object.fromEntries(
        queryable.map((table) => [table.name, (snapshot.schemas[table.name]?.fields ?? []).filter((field) => field.kind !== 'container').map((field) => field.name)]),
      ),
    [queryable, snapshot.schemas],
  );

  const draftKey = viewKey({ table: tableName, sql: sqlMode ? sqlDraft : null, range: rangeLabel(range), filters, order });
  const dirty = run !== null && !starting && draftKey !== requestKey(run.request);

  const execute = useCallback(
    (overrides: { table?: string; filters?: BrowseFilter[] } = {}) => {
      const table = overrides.table ?? tableName;
      const request: RunRequest = sqlMode
        ? { kind: 'sql', table, range: resolveRange(range), rangeLabel: rangeLabel(range), sql: sqlDraft }
        : { kind: 'browse', table, range: resolveRange(range), rangeLabel: rangeLabel(range), filters: overrides.filters ?? filters, order };
      setPage(0);
      setDetailIndex(null);
      setLiveRefused(false);
      setPeekOpen(false);
      runRange.current = range;
      void start(request);
    },
    [sqlMode, range, sqlDraft, filters, order, tableName, start],
  );

  // Another table: its own columns, so filters and column choices of this one do not carry over.
  // Its preview runs at once.
  const chooseTable = useCallback(
    (name: string) => {
      if (name === tableName) return;
      setRequestedTable(name);
      setFilters([]);
      setVisibleOverride(null);
      setPinnedColumn(null);
      execute({ table: name, filters: [] });
    },
    [tableName, execute],
  );

  // Opening the tab: pick the run in the address bar back up, or run the view it describes.
  const opened = useRef(false);
  useEffect(() => {
    if (opened.current || !queryableNow) return;
    opened.current = true;
    void (async () => {
      if (initial.runId && (await attach(initial.runId))) return;
      execute();
    })();
  }, [queryableNow, initial.runId, attach, execute]);

  // ⌘↵ runs from anywhere on the tab; the editor handles its own and marks the event.
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent): void => {
      if (!(event.metaKey || event.ctrlKey) || event.key !== 'Enter' || event.defaultPrevented) return;
      event.preventDefault();
      execute();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [execute]);

  useEffect(() => remember(RAIL_WIDTH_KEY, railWidth), [railWidth]);
  useEffect(() => remember(SQL_HEIGHT_KEY, sqlHeight), [sqlHeight]);

  // Mirror the view into the address bar. Replaced, not pushed: a page turn is not a navigation.
  useEffect(() => {
    const next = writeViewState({
      table: tableName === tables[0]?.name ? null : tableName,
      range,
      filters,
      order,
      sql: sqlMode ? sqlDraft : null,
      page,
      pageSize,
      visibleColumns: visibleOverride,
      pinnedColumn,
      runId: run?.id ?? initial.runId,
      live,
    });
    if (next.toString() !== searchParams.toString()) setSearchParams(next, { replace: true });
  }, [tableName, tables, range, filters, order, sqlMode, sqlDraft, page, pageSize, visibleOverride, pinnedColumn, run?.id, initial.runId, live, searchParams, setSearchParams]);

  // Check a statement as it is typed: bound against the view, never run.
  useEffect(() => {
    if (!sqlMode || sqlDraft.trim() === '') {
      setValidation(null);
      return;
    }
    const statement = sqlDraft;
    const timer = setTimeout(() => {
      api
        .validateSql(materializationId, statement)
        .then(() => setValidation(null))
        .catch((error: unknown) => {
          if (error instanceof ApiError && (error.code === 'query_failed' || error.code === 'invalid_request')) {
            setValidation({
              sql: statement,
              message: error.message,
              position: (error.details.position as StatementPosition | undefined) ?? null,
            });
          }
        });
    }, 600);
    return () => clearTimeout(timer);
  }, [sqlMode, sqlDraft, materializationId]);

  const columns = useMemo<QueryColumn[]>(() => run?.columns ?? [], [run?.columns]);
  // The table on screen completes with the columns its last browse returned, which name forks as
  // the grid does; before one, with its schema's fields.
  const browsedTable = run?.request.kind === 'browse' ? run.table : null;
  const editorColumns = useMemo(
    () => (browsedTable === tableName && columns.length > 0 ? columns.map((column) => column.name) : (tableColumns[tableName] ?? [])),
    [browsedTable, tableName, columns, tableColumns],
  );
  const statementResult = run?.request.kind === 'sql';
  const labels = useMemo(() => buildColumnLabels(columns), [columns]);
  const ordered = useMemo(
    () => (statementResult ? columns : gridOrder(columns, schemaOrder)),
    [statementResult, columns, schemaOrder],
  );

  // A link made before columns dropped the `payload.` root names `payload.status`; once the run's
  // columns are known, every name the view holds is rewritten to the column it means, so the
  // address bar and everything after it use one spelling.
  useEffect(() => {
    if (columns.length === 0) return;
    const resolve = (name: string): string => resolveColumnName(name, columns) ?? name;
    setVisibleOverride((current) => {
      if (current === null) return current;
      const next = current.map(resolve);
      return next.some((name, index) => name !== current[index]) ? next : current;
    });
    setPinnedColumn((current) => (current === null ? current : resolve(current)));
    if (run?.request.kind === 'browse') {
      setFilters((current) => {
        const next = current.map((filter) => ({ ...filter, column: resolve(filter.column) }));
        return next.some((filter, index) => filter.column !== current[index]?.column) ? next : current;
      });
    }
  }, [columns, run?.request.kind]);

  const visible = useMemo(
    () => new Set(statementResult ? columns.map((column) => column.name) : (visibleOverride ?? defaultVisibleColumns(columns))),
    [statementResult, columns, visibleOverride],
  );
  const gridColumns = useMemo(() => {
    const shown = ordered.filter((column) => visible.has(column.name));
    const pinnedIndex = shown.findIndex((column) => column.name === pinnedColumn);
    if (pinnedIndex <= 0) return shown;
    // Freezing only works against the left edge, so the pinned column moves to the front.
    const pinned = shown[pinnedIndex] as QueryColumn;
    return [pinned, ...shown.slice(0, pinnedIndex), ...shown.slice(pinnedIndex + 1)];
  }, [ordered, visible, pinnedColumn]);

  const pageQuery = useRunPage(run, page * pageSize, pageSize);
  const rows = useMemo(() => pageQuery.data?.rows ?? [], [pageQuery.data]);
  // Rows on screen may still be the previous page's while the next one lands; they keep their own
  // numbering and are shown muted, so a row is never labelled with a position it does not hold.
  const shownOffset = pageQuery.data?.offset ?? page * pageSize;
  const stale = pageQuery.isPlaceholderData || shownOffset !== page * pageSize;

  const setVisible = useCallback(
    (next: (current: string[]) => string[]) => setVisibleOverride((current) => next(current ?? defaultVisibleColumns(columns))),
    [columns],
  );
  const addFilter = useCallback((filter: BrowseFilter) => setFilters((current) => [...current, filter]), []);

  const enterSql = useCallback(() => {
    const dataset = datasets.get(tableName);
    setSqlDraft(starterStatement(dataset ? `${domain.id}.${dataset}.${tableName}` : tableName, filters, columns));
    setSqlMode(true);
  }, [datasets, tableName, filters, columns]);

  const editorError = useMemo(() => {
    if (validation && validation.sql === sqlDraft) return validation;
    if (run?.state === 'failed' && run.request.kind === 'sql' && run.request.sql === sqlDraft && run.error) {
      return { message: run.error.message, position: run.error.position ?? null };
    }
    return null;
  }, [validation, sqlDraft, run]);

  // Export needs `data.export` on every table the run reads, each on its dataset, in its domain.
  const exportable =
    run !== null &&
    (run.tables.length > 0
      ? run.tables.every((table) => consoleAuth.forDomain(table.domain).can('data.export', table.datasetId))
      : tables.some((table) => table.name === run.table && can('data.export', table.dataset_id)));

  const shareView = useCallback((): void => {
    void navigator.clipboard.writeText(window.location.href);
    toast.success('Link copied', 'It reopens this exact view.');
  }, [toast]);

  // Live: the browse view asks for the tail's newest records every few seconds, while it is seen.
  const liveRefreshMs = domainTail.refreshMs;
  const hold = liveHold({
    enabled: liveTail && live && !sqlMode,
    visible: pageVisible,
    run,
    authRefused: liveRefused,
    inspecting: detailIndex !== null,
    page,
  });
  useEffect(() => {
    if (hold !== null || run === null) return;
    const timer = window.setTimeout(() => {
      void refresh(resolveRange(runRange.current))
        .catch((error: unknown) => {
          if (!(error instanceof ApiError)) return;
          if (error.status === 401 || error.status === 403) setLiveRefused(true);
          // The run expired while nobody looked: start the view again.
          else if (error.status === 404 && error.details.resource === 'run') execute();
        })
        .finally(() => setLiveTick((tick) => tick + 1));
    }, liveRefreshMs);
    return () => window.clearTimeout(timer);
    // One look per tick: the run changes with every answer, and must not start a second timer.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hold, run?.id, liveTick, liveRefreshMs]);

  // A statement's Auto re-run: a new run a few seconds after the last one finished.
  useEffect(() => {
    const delay = autoRerunDelay({ enabled: liveTail && sqlMode && autoRerun, visible: pageVisible, run, dirty });
    if (delay === null) return;
    const timer = window.setTimeout(() => {
      void refresh(resolveRange(runRange.current)).catch((error: unknown) => {
        if (error instanceof ApiError && (error.status === 401 || error.status === 403)) setAutoRerun(false);
      });
    }, delay);
    return () => window.clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveTail, sqlMode, autoRerun, pageVisible, run?.id, run?.state, dirty]);

  // What the tab says of Live, on the status line: a muted word, or a warning in amber at its right.
  // The warning describes the table, not one run, so it stays while a run that Live started at a
  // commit works, rather than leaving and coming back.
  // How far the table's topic is ahead of the table, as the Overview measures it: the backlog's
  // count follows it at the Overview's pace, whether or not Live refreshes the run.
  const tableTopic = tables.find((table) => table.name === tableName)?.source_topic;
  const topicLag = tableTopic ? (snapshot.detail?.status.consumer_lag?.topics?.[tableTopic]?.total_lag ?? null) : null;
  const notice = liveTail
    ? tailNotice(run?.tail, { live: live && !sqlMode, hold, maxRows: domainTail.maxRows, lag: topicLag })
    : null;
  const [liveWarning, setLiveWarning] = useState<TailNotice | null>(null);
  const runFinished = run !== null && run.state !== 'running';
  const warningKey = notice?.tone === 'warning' ? JSON.stringify(notice) : null;
  useEffect(() => {
    if (!liveTail) setLiveWarning(null);
    else if (runFinished) setLiveWarning(warningKey === null ? null : (JSON.parse(warningKey) as TailNotice));
  }, [liveTail, runFinished, warningKey]);

  // The peek is about a backlogged table: closed once a finished run has caught up with the head.
  const peekNoLongerNeeded = peekDone(run);
  useEffect(() => {
    if (peekNoLongerNeeded) setPeekOpen(false);
  }, [peekNoLongerNeeded]);
  const closePeek = useCallback(() => setPeekOpen(false), []);

  if (summaryQuery.isPending) {
    return (
      <PageBody className="space-y-3">
        <Skeleton className="h-10" />
        <Skeleton className="h-[560px]" />
      </PageBody>
    );
  }

  if (!queryableNow) {
    const failure = summaryQuery.error;
    const reason = failure instanceof ApiError ? failure.details.reason : undefined;
    return (
      <PageBody>
        <Panel>
          {tables.length > 1 ? (
            <div className="flex items-center border-b border-[var(--color-line)] px-3 py-2">
              <TablePicker tables={tables} value={tableName} onChange={chooseTable} locked={(table) => !mayQuery(table)} />
            </div>
          ) : null}
          <EmptyState
            icon={<Table2 className="size-6" />}
            title={failure ? `${tableName || 'The table'} is not readable` : tables.length > 1 ? `No data in ${tableName} yet` : 'No data to query yet'}
            description={
              failure
                ? failure instanceof ApiError
                  ? `${failure.message}${typeof reason === 'string' ? `: ${reason}` : ''}`
                  : 'The console could not read this table.'
                : 'Rows appear here once the first records are saved.'
            }
            action={
              <Button onClick={() => void summaryQuery.refetch()}>
                <RefreshCw className="size-4" />
                {failure ? 'Retry' : 'Check again'}
              </Button>
            }
          />
        </Panel>
      </PageBody>
    );
  }

  const rowsOnPage = rows.length;
  const browseRun = run?.request.kind === 'browse';

  return (
    <PageBody>
      <div className="flex h-[calc(100dvh-196px)] min-h-[560px] flex-col gap-2">
        {sqlMode ? (
          <>
            <Panel className="flex shrink-0 flex-col overflow-hidden" style={{ height: sqlHeight }}>
              <SqlPanel
                sql={sqlDraft}
                tableName={tableName}
                domain={domain.id}
                tables={queryable}
                otherTables={otherTables}
                tableColumns={tableColumns}
                rangeLabel={rangeLabel(range)}
                columns={editorColumns}
                error={editorError}
                onChange={setSqlDraft}
                onRun={() => execute()}
                onExit={() => setSqlMode(false)}
                autoRerun={liveTail ? { on: autoRerun, onChange: setAutoRerun } : null}
              />
            </Panel>
            <ResizeHandle
              label="Resize the query editor"
              value={sqlHeight}
              min={SQL_MIN_HEIGHT}
              max={SQL_MAX_HEIGHT}
              onResize={(delta) => setSqlHeight((current) => Math.min(SQL_MAX_HEIGHT, Math.max(SQL_MIN_HEIGHT, current + delta)))}
            />
          </>
        ) : null}

        <Panel className="relative flex min-h-0 flex-1 flex-col overflow-hidden">
          {running ? <span className="run-ring" aria-hidden /> : null}

          <div className="flex flex-wrap items-center gap-2 border-b border-[var(--color-line)] px-3 py-2">
            <Button
              size="sm"
              variant="ghost"
              className="h-8 shrink-0 px-1.5"
              onClick={() => setRailOpen((value) => !value)}
              aria-label={railOpen ? 'Hide columns' : 'Show columns'}
              title={railOpen ? 'Hide columns' : 'Show columns'}
            >
              {railOpen ? <PanelLeftClose className="size-4" /> : <PanelLeftOpen className="size-4" />}
            </Button>

            {sqlMode ? null : <TablePicker tables={tables} value={tableName} onChange={chooseTable} locked={(table) => !mayQuery(table)} />}
            <TimeRangePicker materializationId={materializationId} table={tableName} value={range} onChange={setRange} summary={summary} />

            {sqlMode ? (
              <span className="text-[12px] text-[var(--color-ink-muted)]">Writing SQL — filters are part of the statement.</span>
            ) : (
              <>
                <Select
                  value={order}
                  onChange={(event) => setOrder(event.target.value as RunOrder)}
                  aria-label="Row order"
                  className="h-8 w-[136px] shrink-0 text-[12px]"
                >
                  <option value="newest">Newest first</option>
                  <option value="oldest">Oldest first</option>
                </Select>
                <FilterChips
                  columns={ordered}
                  labels={labels}
                  filters={filters}
                  onChange={setFilters}
                  runId={browseRun ? (run?.id ?? null) : null}
                />
              </>
            )}

            <span className="ml-auto flex shrink-0 items-center gap-2">
              {liveTail && !sqlMode ? (
                <button
                  type="button"
                  aria-pressed={live}
                  onClick={() => setLive((value) => !value)}
                  title={live ? 'Stop following new records' : 'Follow new records as they arrive'}
                  className={cn(
                    'inline-flex h-8 shrink-0 items-center gap-1.5 rounded-[var(--radius-control)] px-2 text-[12px] transition-colors',
                    live ? 'font-medium text-[var(--color-ink)]' : 'text-[var(--color-ink-muted)] hover:bg-[var(--color-raised)]',
                  )}
                  data-role="live-toggle"
                >
                  <span
                    aria-hidden
                    className={cn(
                      'size-2 rounded-full',
                      live && hold === null ? 'animate-pulse bg-[var(--color-phase-running)]' : live ? 'bg-[var(--color-phase-paused)]' : 'bg-[var(--color-ink-faint)]',
                    )}
                  />
                  Live
                </button>
              ) : null}
              {sqlMode ? null : (
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-8 px-2 text-[12px]"
                  onClick={enterSql}
                  title={tables.length > 1 ? 'Write SQL against this table, or join it with the others' : 'Write SQL against this data'}
                >
                  <Code2 className="size-3.5" />
                  SQL
                </Button>
              )}
              <button
                type="button"
                onClick={running ? cancel : () => execute()}
                disabled={starting}
                title={running ? 'Stop the running query' : `Run (${RUN_HINT})`}
                className={cn(
                  'relative inline-flex h-8 shrink-0 items-center gap-1.5 rounded-[var(--radius-control)] px-3.5 text-[12px] font-medium text-white transition-[filter]',
                  'bg-[var(--color-accent)] hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60',
                  'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-accent)]',
                  dirty && !running && 'ring-2 ring-[var(--color-accent-soft)] ring-offset-1 ring-offset-[var(--color-surface)]',
                )}
              >
                {running ? <Square className="size-3 fill-current" /> : <Play className="size-3.5 fill-current" />}
                {running ? 'Stop' : 'Run'}
              </button>
              <MoreActions
                run={run}
                config={configQuery.data ?? null}
                exportable={exportable}
                onShare={shareView}
                onPeek={liveWarning?.peek && run?.request.kind === 'browse' ? () => setPeekOpen(true) : null}
              />
            </span>
          </div>

          <StatusBar
            run={run}
            starting={starting}
            summary={summary}
            dirty={dirty}
            onRerun={() => execute()}
            tailNotice={notice?.tone === 'muted' ? notice : null}
            warning={liveWarning}
          />

          <div className="flex min-h-0 flex-1">
            {railOpen ? (
              <>
                <ColumnRail
                  columns={ordered}
                  labels={labels}
                  schemaOrder={schemaOrder}
                  visible={visible}
                  onToggle={(column) =>
                    setVisible((current) => (current.includes(column) ? current.filter((name) => name !== column) : [...current, column]))
                  }
                  onShowOnly={(column) => setVisibleOverride([column])}
                  pinnedColumn={pinnedColumn}
                  onPin={(column) => {
                    setPinnedColumn(column);
                    // Freezing a hidden column would do nothing, so pinning also reveals it.
                    if (column !== null && !visible.has(column)) setVisible((current) => [...current, column]);
                  }}
                  onSetGroup={(group: ColumnGroupId, show) =>
                    setVisible((current) => {
                      const inGroup = columns.filter((column) => groupOf(column.name) === group).map((column) => column.name);
                      return show ? [...new Set([...current, ...inGroup])] : current.filter((name) => !inGroup.includes(name));
                    })
                  }
                  runId={run && run.state !== 'failed' ? run.id : null}
                  onFilter={sqlMode ? undefined : addFilter}
                  width={railWidth}
                />
                <ResizeHandle
                  orientation="vertical"
                  label="Resize the column list"
                  value={railWidth}
                  min={RAIL_MIN_WIDTH}
                  max={RAIL_MAX_WIDTH}
                  onResize={(delta) => setRailWidth((current) => Math.min(RAIL_MAX_WIDTH, Math.max(RAIL_MIN_WIDTH, current + delta)))}
                />
              </>
            ) : null}

            <div className="flex min-w-0 flex-1 flex-col">
              <div className="relative flex min-h-0 flex-1 flex-col">
                {peekOpen && run ? (
                  <HeadPeek
                    materializationId={materializationId}
                    table={run.table}
                    refreshMs={liveRefreshMs}
                    visible={pageVisible}
                    schemaOrder={schemaOrder}
                    onClose={closePeek}
                  />
                ) : null}
                <GridArea
                  run={run}
                  startError={startError}
                  loading={pageQuery.isLoading || (run?.state === 'running' && rowsOnPage === 0)}
                  pageError={pageQuery.error}
                  columns={gridColumns}
                  rows={rows}
                  rowOffset={shownOffset}
                  stale={stale}
                  labels={labels}
                  pinnedColumn={statementResult ? null : pinnedColumn}
                  range={range}
                  hasFilters={!sqlMode && filters.length > 0}
                  summarySpan={
                    summary?.timeMin && summary.timeMax
                      ? `${formatTimestamp(summary.timeMin)} to ${formatTimestamp(summary.timeMax)}`
                      : null
                  }
                  onExpand={setDetailIndex}
                  onCopy={(column, value) => {
                    void navigator.clipboard.writeText(value === null || value === undefined ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value));
                    toast.success('Copied', labelFor(labels, column).display);
                  }}
                  onSearchAllTime={() => setRange(ALL_TIME)}
                />
              </div>
              <Pager
                run={run}
                page={page}
                pageSize={pageSize}
                onPage={(next) => {
                  setPage(next);
                  setDetailIndex(null);
                }}
                onPageSize={(size) => {
                  setPageSize(size);
                  setPage(0);
                }}
              />
            </div>
          </div>
        </Panel>
      </div>

      {detailIndex !== null && rows[detailIndex] ? (
        <RowDrawer
          row={rows[detailIndex] as Record<string, unknown>}
          columns={ordered}
          schemaOrder={schemaOrder}
          index={page * pageSize + detailIndex}
          total={run?.rows ?? rows.length}
          onClose={() => setDetailIndex(null)}
          onStep={(delta) =>
            setDetailIndex((current) => {
              if (current === null) return current;
              const next = current + delta;
              return next < 0 || next >= rows.length ? current : next;
            })
          }
          onFilter={(filter) => {
            if (sqlMode) return;
            addFilter(filter);
            setDetailIndex(null);
          }}
        />
      ) : null}
    </PageBody>
  );
}

/**
 * The grid, or what to say instead of it.
 *
 * "No rows" is never left unexplained: an empty range says where the data actually is, and offers
 * the range that has it.
 */
function GridArea({
  run,
  startError,
  loading,
  pageError,
  columns,
  rows,
  rowOffset,
  stale,
  labels,
  pinnedColumn,
  range,
  hasFilters,
  summarySpan,
  onExpand,
  onCopy,
  onSearchAllTime,
}: {
  run: ReturnType<typeof useRun>['run'];
  startError: ApiError | null;
  loading: boolean;
  pageError: unknown;
  columns: QueryColumn[];
  rows: Record<string, unknown>[];
  rowOffset: number;
  stale: boolean;
  labels: ReturnType<typeof buildColumnLabels>;
  pinnedColumn: string | null;
  range: TimeRangeSpec;
  hasFilters: boolean;
  summarySpan: string | null;
  onExpand: (index: number) => void;
  onCopy: (column: string, value: unknown) => void;
  onSearchAllTime: () => void;
}): ReactNode {
  if (startError) {
    return <Notice tone="failed" title="The query could not start" description={startError.message} />;
  }
  if (run?.state === 'failed') {
    return (
      <Notice
        tone="failed"
        title={run.request.kind === 'sql' ? 'The statement failed' : 'The query failed'}
        description={run.error?.message ?? 'Unknown error'}
      />
    );
  }
  if (pageError) {
    return <Notice tone="failed" title="This page could not be read" description={pageError instanceof Error ? pageError.message : String(pageError)} />;
  }
  if (run && run.state !== 'running' && run.rows === 0) {
    const scoped = !sameRange(range, ALL_TIME);
    return (
      <EmptyState
        icon={<Table2 className="size-6" />}
        title={run.state === 'cancelled' ? 'Cancelled before any rows were found' : 'No rows match'}
        description={
          run.request.kind === 'sql'
            ? 'The statement returned no rows.'
            : scoped
              ? `Nothing has a Kafka timestamp in ${describeInstants(run.request.range)}${hasFilters ? ' that passes these filters' : ''}.${summarySpan ? ` Records span ${summarySpan}.` : ''}`
              : 'No row passes these filters. Remove one, or widen the time range.'
        }
        action={
          scoped && run.request.kind === 'browse' ? (
            <Button size="sm" variant="secondary" onClick={onSearchAllTime}>
              Use All time
            </Button>
          ) : undefined
        }
        className="py-20"
      />
    );
  }
  if (columns.length === 0 && run && run.columns && run.columns.length > 0) {
    return (
      <EmptyState
        icon={<Table2 className="size-6" />}
        title="No columns selected"
        description="Pick columns in the list on the left to see rows."
        className="py-20"
      />
    );
  }
  // A placeholder grid would promise rows that may never come; say what the scan has found so far.
  if (run?.state === 'running' && run.request.kind === 'browse' && run.rows === 0 && run.progress.objectsDone > 0) {
    return (
      <EmptyState
        icon={<Table2 className="size-6" />}
        title="No matching rows yet"
        description="Still scanning. Rows appear here as soon as one matches."
        className="py-20"
      />
    );
  }
  if (rows.length === 0 && (loading || !run || run.state === 'running')) {
    return <LoadingRows />;
  }
  return (
    <DataGrid
      columns={columns}
      rows={rows}
      onRowExpand={onExpand}
      rowNumberOffset={rowOffset}
      dimmed={stale}
      labels={labels}
      pinnedColumn={pinnedColumn}
      onCellClick={onCopy}
      emptyMessage="No rows on this page"
    />
  );
}

/** Whether the page is being seen: a hidden tab follows nothing. */
function useDocumentVisible(): boolean {
  const [visible, setVisible] = useState(() => typeof document === 'undefined' || document.visibilityState !== 'hidden');
  useEffect(() => {
    const onChange = (): void => setVisible(document.visibilityState !== 'hidden');
    document.addEventListener('visibilitychange', onChange);
    return () => document.removeEventListener('visibilitychange', onChange);
  }, []);
  return visible;
}

function LoadingRows(): ReactNode {
  return (
    <div className="space-y-2 p-3" aria-label="Loading rows">
      {Array.from({ length: 8 }, (_, index) => (
        <Skeleton key={index} className="h-6" />
      ))}
    </div>
  );
}

function Notice({ tone, title, description }: { tone: 'failed'; title: string; description: string }): ReactNode {
  return (
    <div className="m-3 rounded-[var(--radius-control)] border border-[color-mix(in_srgb,var(--color-phase-failed)_35%,transparent)] bg-[color-mix(in_srgb,var(--color-phase-failed)_8%,transparent)] px-3 py-2.5">
      <p className={cn('text-[12px] font-medium', tone === 'failed' && 'text-[var(--color-phase-failed)]')}>{title}</p>
      <p className="numeric mt-1 text-[12px] whitespace-pre-wrap text-[var(--color-ink)]">{description}</p>
    </div>
  );
}

/**
 * A statement to start from: the filters already chosen, written out, over the table on screen by
 * its full name, `domain_a.shop.orders` (handbook 20).
 *
 * The time range is not written in, because it already bounds the table itself.
 */
function starterStatement(tableName: string, filters: BrowseFilter[], columns: QueryColumn[]): string {
  const conditions = filters.map((filter) => filterSql(filter, columns.find((column) => column.name === filter.column)?.type ?? ''));
  const where = conditions.length > 0 ? `\nWHERE ${conditions.join('\n  AND ')}` : '';
  return `SELECT *\nFROM ${tableName}${where}\nLIMIT 1000`;
}

function filterSql(filter: BrowseFilter, type: string): string {
  const column = `"${filter.column.replace(/"/g, '""')}"`;
  const literal = `'${(filter.value ?? '').replace(/'/g, "''")}'`;
  const like = (pattern: string): string => `CAST(${column} AS VARCHAR) ILIKE '${pattern.replace(/'/g, "''")}'`;
  const escaped = (filter.value ?? '').replace(/[\\%_]/g, (match) => `\\${match}`);
  switch (filter.operator) {
    case 'is_null':
      return `${column} IS NULL`;
    case 'is_not_null':
      return `${column} IS NOT NULL`;
    case 'contains':
      return like(`%${escaped}%`);
    case 'starts_with':
      return like(`${escaped}%`);
    case 'eq':
      return isListType(type) ? `list_contains(${column}, ${literal})` : `${column} = ${literal}`;
    case 'neq':
      return isListType(type) ? `NOT list_contains(${column}, ${literal})` : `${column} <> ${literal}`;
    case 'gt':
      return `${column} > ${literal}`;
    case 'gte':
      return `${column} >= ${literal}`;
    case 'lt':
      return `${column} < ${literal}`;
    case 'lte':
      return `${column} <= ${literal}`;
  }
}
