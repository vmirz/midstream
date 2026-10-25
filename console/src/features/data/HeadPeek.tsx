import { useQuery } from '@tanstack/react-query';
import { Eye, Loader2, X } from 'lucide-react';
import { useEffect, useMemo, useRef, type ReactNode } from 'react';

import { buildColumnLabels, defaultVisibleColumns, gridOrder } from './columns';
import { DataGrid } from '@/components/DataGrid';
import { Button } from '@/components/ui/primitives';
import { ApiError, api } from '@/lib/api';
import { formatExact } from '@/lib/format';

/** Records one peek asks for: the newest on the topic, newest first. */
const PEEK_ROWS = 200;

/**
 * The newest records on a table's topic, while its runtime is too far behind for the live tail.
 *
 * A backlogged table — a new materialization's backfill, a burst that outgrew its bound — has no
 * tail: its runtime is millions of records from the head. This shows what is arriving at the head
 * meanwhile, in the table's own columns, labelled as not in the table yet. It is never part of the
 * run on screen, its exports or its profiles. It reads again every `refreshMs` while open and the
 * tab is seen, and closes itself once a table it saw backlogged has caught up and the run's tail
 * takes over. Opened for a tail that is only short of the head, it stays until it is closed or the
 * run's tail reaches the head.
 */
export function HeadPeek({
  materializationId,
  table,
  refreshMs,
  visible,
  schemaOrder,
  onClose,
}: {
  materializationId: string;
  table: string;
  refreshMs: number;
  visible: boolean;
  /** The table's fields in schema order, which the grid follows. */
  schemaOrder: readonly string[];
  onClose: () => void;
}): ReactNode {
  const query = useQuery({
    queryKey: ['head-peek', materializationId, table],
    queryFn: () => api.tableHead(materializationId, table, PEEK_ROWS),
    refetchInterval: visible ? refreshMs : false,
    retry: 0,
  });
  const state = query.data?.peek.state ?? null;
  const sawBacklog = useRef(false);
  useEffect(() => {
    if (state === 'backlogged') sawBacklog.current = true;
    // Caught up: the run's live tail shows these records again, in the table itself.
    else if (state !== null && state !== 'seeding' && sawBacklog.current) onClose();
  }, [state, onClose]);

  const columns = useMemo(() => {
    const all = query.data?.columns ?? [];
    const shown = new Set(defaultVisibleColumns(all));
    return gridOrder(all, [...schemaOrder]).filter((column) => shown.has(column.name));
  }, [query.data?.columns, schemaOrder]);
  const labels = useMemo(() => buildColumnLabels(query.data?.columns ?? []), [query.data?.columns]);
  const peek = query.data?.peek;
  const failure = query.error instanceof ApiError ? query.error.message : query.error ? String(query.error) : null;

  return (
    <div
      className="absolute inset-0 z-20 flex flex-col border-t-2 border-[var(--color-phase-paused)] bg-[var(--color-surface)]"
      role="dialog"
      aria-label="Newest incoming records"
      data-role="head-peek"
    >
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--color-line)] px-3 py-2">
        <Eye className="size-4 shrink-0 text-[var(--color-phase-paused)]" aria-hidden />
        <span className="text-[13px] font-medium text-[var(--color-ink)]">Newest incoming records</span>
        <span className="rounded-full bg-[color-mix(in_srgb,var(--color-phase-paused)_14%,transparent)] px-2 py-0.5 text-[11px] font-medium text-[var(--color-phase-paused)]">
          not in the table yet
        </span>
        <span className="numeric min-w-0 truncate text-[11px] text-[var(--color-ink-muted)]">
          {peek
            ? `${formatExact(peek.rows)} newest · ${formatExact(peek.behind)} records behind${peek.settling > 0 ? ` · ${formatExact(peek.settling)} with new fields` : ''}`
            : failure ?? 'Loading…'}
        </span>
        {/* The first read only: a spinner on every re-read would flash each `refreshMs`. */}
        {query.isPending ? <Loader2 className="size-3.5 animate-spin text-[var(--color-ink-faint)]" aria-hidden /> : null}
        <Button size="sm" variant="ghost" className="ml-auto h-7 px-2 text-[12px]" onClick={onClose} title="Close the peek">
          <X className="size-3.5" />
          Close
        </Button>
      </div>
      <div className="flex min-h-0 flex-1 flex-col">
        {failure && !peek ? (
          <p className="m-3 text-[12px] text-[var(--color-phase-failed)]">{failure}</p>
        ) : (
          <DataGrid columns={columns} rows={query.data?.rows ?? []} labels={labels} emptyMessage={query.isPending ? 'Loading…' : 'No incoming records'} />
        )}
      </div>
    </div>
  );
}
