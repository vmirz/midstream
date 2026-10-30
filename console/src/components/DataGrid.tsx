import { ArrowDown, ArrowUp, Maximize2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { ColumnLabelText } from '@/features/data/ColumnLabelText';
import { ProtectedMark } from '@/features/protection/ProtectionSummary';
import { labelFor, type ColumnLabel } from '@/features/data/columns';
import { cn } from '@/lib/cn';
import { displayCell, isInstantType, localZone } from '@/lib/format';
import type { QueryColumn } from '@shared/types';

const ROW_HEIGHT = 32;
const ROW_OVERSCAN = 12;
/** Horizontal slack rendered beyond each edge of the viewport, so a short scroll never shows a gap. */
const COLUMN_OVERSCAN_PX = 480;
/** Leading gutter holding the absolute row number and the expand affordance. */
const GUTTER_WIDTH = 52;

export interface DataGridProps {
  columns: QueryColumn[];
  rows: Record<string, unknown>[];
  sort?: { column: string; direction: 'asc' | 'desc' } | null;
  onSortChange?: (column: string) => void;
  onCellClick?: (column: string, value: unknown) => void;
  /** Opens the row detail view. Enables the leading gutter when provided. */
  onRowExpand?: (index: number) => void;
  /** Absolute index of the first row, so numbering continues across pages. */
  rowNumberOffset?: number;
  /** Display labels keyed by column name; falls back to the raw name. */
  labels?: Map<string, ColumnLabel>;
  /** Column frozen against the gutter. Only honoured when the caller renders it first. */
  pinnedColumn?: string | null;
  /** The rows are being replaced; show them muted rather than as current. */
  dimmed?: boolean;
  emptyMessage?: string;
  className?: string;
  /** Fixed viewport height. Omit to fill the parent, which must bound the grid's height. */
  height?: number;
}

/**
 * Windowed result grid, in both directions.
 *
 * Results reach thousands of rows and, for a wide schema, dozens of columns, so only the cells in
 * view are mounted: the rows inside the viewport plus a margin, and likewise the columns. A page of
 * an eighty-column table is about eight columns wide on screen, and rendering all eighty for every
 * visible row was most of the cost of turning a page. Row height is fixed to keep the vertical
 * math exact; column widths are measured once per result and summed for the horizontal math.
 *
 * With no `height` the grid fills its parent and measures itself. Filling also drops the grid's
 * own border and rounding: it is then the panel's content rather than a card inside it.
 */
export function DataGrid({
  columns,
  rows,
  sort,
  onSortChange,
  onCellClick,
  onRowExpand,
  rowNumberOffset = 0,
  labels,
  pinnedColumn,
  dimmed = false,
  emptyMessage = 'No rows',
  className,
  height,
}: DataGridProps): ReactNode {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [scroll, setScroll] = useState({ top: 0, left: 0 });
  // Instants render in the reader's zone, named once in their column header.
  const zone = useMemo(() => localZone(), []);
  const [measured, setMeasured] = useState({ width: 0, height: 0 });
  const fills = height === undefined;

  const onScroll = useCallback(() => {
    const element = viewportRef.current;
    if (element) setScroll({ top: element.scrollTop, left: element.scrollLeft });
  }, []);

  useEffect(() => {
    const element = viewportRef.current;
    if (!element) return;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (rect) setMeasured({ width: rect.width, height: rect.height });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const viewportHeight = fills ? measured.height : (height ?? 0);

  const { start, end } = useMemo(() => {
    const first = Math.max(0, Math.floor(scroll.top / ROW_HEIGHT) - ROW_OVERSCAN);
    const visible = Math.ceil(Math.max(viewportHeight, ROW_HEIGHT) / ROW_HEIGHT) + ROW_OVERSCAN * 2;
    return { start: first, end: Math.min(rows.length, first + visible) };
  }, [scroll.top, rows.length, viewportHeight]);

  // Widths follow the columns and a sample of rows, so they hold steady while paging through
  // one result instead of jumping with every page.
  const sampleKey = rows.length > 0 ? columns.length : 0;
  const widths = useMemo(
    () =>
      columns.map((column) => {
        const sample = rows.slice(0, 40).map((row) => displayCell(row[column.name], column.type).length);
        // The header is 11px and cells are 12px, and the header also reserves room for the sort
        // arrow and an instant's zone — so the two are measured separately rather than with one
        // fudge factor.
        const label = labels ? labelFor(labels, column.name).display : column.name;
        const zonePx = isInstantType(column.type) ? zone.label.length * 6.9 + 6 : 0;
        const headerPx = label.length * 6.9 + 46 + zonePx;
        const cellPx = Math.max(0, ...sample) * 7.3 + 26;
        return Math.round(Math.min(Math.max(headerPx, cellPx, 110), 420));
      }),
    // `rows` is deliberately not a dependency: widths are measured per result, not per page.
    [columns, labels, sampleKey, zone],
  );

  const gutter = onRowExpand ? GUTTER_WIDTH : 0;
  // A pinned column freezes beside the gutter, so it must be the first one rendered.
  const pinnedIndex = pinnedColumn != null && columns[0]?.name === pinnedColumn ? 0 : -1;

  /*
   * The columns to mount: everything overlapping the viewport plus a margin, and always the pinned
   * one. Spacer tracks on either side stand in for the rest, so the scroll width and every
   * column's position are exactly what they would be if all of them were rendered.
   */
  const layout = useMemo(() => {
    const lefts: number[] = [];
    let total = 0;
    for (const width of widths) {
      lefts.push(total);
      total += width;
    }
    const viewportWidth = measured.width || 1600;
    const from = scroll.left - COLUMN_OVERSCAN_PX;
    const to = scroll.left + viewportWidth + COLUMN_OVERSCAN_PX;
    const scrollable = widths.map((_, index) => index).filter((index) => index !== pinnedIndex);
    const shown = scrollable.filter((index) => {
      const left = (lefts[index] ?? 0) + gutter;
      return left + (widths[index] ?? 0) >= from && left <= to;
    });
    const firstShown = shown[0];
    const lastShown = shown.at(-1);
    const pinnedWidth = pinnedIndex === 0 ? (widths[0] ?? 0) : 0;
    const leftSpacer =
      firstShown === undefined ? 0 : (lefts[firstShown] ?? 0) - pinnedWidth;
    const rightSpacer =
      lastShown === undefined ? 0 : total - ((lefts[lastShown] ?? 0) + (widths[lastShown] ?? 0));
    const mounted = pinnedIndex === 0 ? [0, ...shown] : shown;
    const tracks = [
      gutter > 0 ? `${gutter}px` : null,
      pinnedIndex === 0 ? `${pinnedWidth}px` : null,
      leftSpacer > 0 ? `${leftSpacer}px` : null,
      ...shown.map((index) => `${widths[index]}px`),
      rightSpacer > 0 ? `${rightSpacer}px` : null,
    ].filter((track): track is string => track !== null);
    return { mounted, leftSpacer, rightSpacer, template: tracks.join(' ') };
  }, [widths, measured.width, scroll.left, gutter, pinnedIndex]);

  if (columns.length === 0) {
    return (
      <div className={cn('flex items-center justify-center py-16 text-[13px] text-[var(--color-ink-muted)]', className)}>
        {emptyMessage}
      </div>
    );
  }

  const isPinned = (index: number): boolean => index === pinnedIndex;
  const spacer = (key: string): ReactNode => <div key={key} aria-hidden />;

  // `isolate` keeps the frozen header's and gutter's z-indexes inside the grid. Without it they
  // stacked over whatever was laid on top of the grid: the Data tab's head peek lost its title bar
  // and its Close button under the column headers of the grid beneath it.
  return (
    <div
      className={cn(
        'isolate flex flex-col overflow-hidden',
        fills ? 'h-full' : 'rounded-[var(--radius-panel)] border border-[var(--color-line)]',
        className,
      )}
    >
      {/*
        One scroll container for both axes. `position: sticky` resolves against the nearest
        scrolling ancestor, so splitting the axes across two elements gave the frozen gutter a
        different scroller from the one it needed, and it drifted away on horizontal scroll.
      */}
      <div
        ref={viewportRef}
        onScroll={onScroll}
        className={cn('overflow-auto', fills && 'min-h-0 flex-1')}
        style={fills ? undefined : { height }}
      >
        <div className="min-w-max">
          <div
            className="sticky top-0 z-20 grid border-b border-[var(--color-line)] bg-[var(--color-raised)]"
            style={{ gridTemplateColumns: layout.template }}
          >
            {onRowExpand ? (
              <div className="sticky left-0 z-30 h-9 border-r border-[var(--color-line)] bg-[var(--color-raised)]" aria-hidden />
            ) : null}
            {layout.mounted.map((index, position) => {
              const column = columns[index] as QueryColumn;
              const active = sort?.column === column.name;
              const pinned = isPinned(index);
              const header = (
                <button
                  key={column.name}
                  type="button"
                  disabled={!onSortChange}
                  onClick={() => onSortChange?.(column.name)}
                  title={
                    isInstantType(column.type)
                      ? `${column.name} · ${column.type} · shown in ${zone.name}`
                      : `${column.name} · ${column.type}`
                  }
                  style={pinned ? { left: gutter } : undefined}
                  className={cn(
                    'flex h-9 items-center gap-1 overflow-hidden px-3 text-left',
                    'text-[11px] font-medium tracking-wide whitespace-nowrap',
                    active ? 'text-[var(--color-ink)]' : 'text-[var(--color-ink-muted)]',
                    onSortChange && 'transition-colors hover:text-[var(--color-ink)]',
                    pinned && 'sticky z-30 border-r border-[var(--color-line)] bg-[var(--color-raised)]',
                  )}
                >
                  {labels ? (
                    <ColumnLabelText labels={labels} name={column.name} className="min-w-0" />
                  ) : (
                    <span className="truncate">{column.name}</span>
                  )}
                  <ProtectedMark protection={column.protection} />
                  {isInstantType(column.type) ? (
                    <span className="shrink-0 font-normal text-[var(--color-ink-faint)]">{zone.label}</span>
                  ) : null}
                  {active ? (
                    sort?.direction === 'asc' ? <ArrowUp className="size-3 shrink-0" /> : <ArrowDown className="size-3 shrink-0" />
                  ) : null}
                </button>
              );
              // The left spacer sits after the pinned column, ahead of the first scrolling one.
              const startsScrolling = position === (pinnedIndex === 0 ? 1 : 0);
              return startsScrolling && layout.leftSpacer > 0 ? [spacer('left'), header] : header;
            })}
            {layout.rightSpacer > 0 ? spacer('right') : null}
          </div>

          {rows.length === 0 ? (
            <div className="flex items-center justify-center py-16 text-[13px] text-[var(--color-ink-muted)]">
              {emptyMessage}
            </div>
          ) : (
            <div
              style={{ height: rows.length * ROW_HEIGHT, position: 'relative' }}
              className={cn('transition-opacity duration-150', dimmed && 'opacity-45')}
              aria-busy={dimmed}
            >
              {/*
                Offset with `top`, not `transform`: a transformed ancestor becomes the containing
                block for its sticky descendants, which would break the frozen gutter again.
              */}
              <div style={{ position: 'absolute', top: start * ROW_HEIGHT, left: 0 }}>
                {rows.slice(start, end).map((row, offset) => {
                  const index = start + offset;
                  return (
                    <div
                      key={index}
                      className="group/row grid border-b border-[var(--color-line)] last:border-b-0 hover:bg-[var(--color-raised)]"
                      style={{ gridTemplateColumns: layout.template, height: ROW_HEIGHT }}
                    >
                      {onRowExpand ? (
                        <button
                          type="button"
                          onClick={() => onRowExpand(index)}
                          title="Open row detail"
                          aria-label={`Open row ${rowNumberOffset + index + 1}`}
                          className="sticky left-0 z-10 flex items-center justify-end gap-1 border-r border-[var(--color-line)] bg-[var(--color-surface)] px-2 text-[var(--color-ink-muted)] transition-colors group-hover/row:bg-[var(--color-raised)] hover:text-[var(--color-ink)]"
                        >
                          <Maximize2 className="size-3 opacity-0 transition-opacity group-hover/row:opacity-100" />
                          <span className="numeric text-[11px]">{rowNumberOffset + index + 1}</span>
                        </button>
                      ) : null}
                      {layout.mounted.map((columnIndex, position) => {
                        const column = columns[columnIndex] as QueryColumn;
                        const value = row[column.name];
                        const empty = value === null || value === undefined;
                        const text = empty ? 'null' : displayCell(value, column.type);
                        const local = !empty && typeof value === 'string' && isInstantType(column.type);
                        const pinned = isPinned(columnIndex);
                        const cell = (
                          <div
                            key={column.name}
                            role={onCellClick ? 'button' : undefined}
                            tabIndex={onCellClick ? 0 : undefined}
                            title={empty ? undefined : local ? `${text} ${zone.label} · ${value}` : text}
                            onClick={onCellClick ? () => onCellClick(column.name, value) : undefined}
                            onKeyDown={
                              onCellClick
                                ? (event) => {
                                    if (event.key === 'Enter' || event.key === ' ') {
                                      event.preventDefault();
                                      onCellClick(column.name, value);
                                    }
                                  }
                                : undefined
                            }
                            style={pinned ? { left: gutter } : undefined}
                            className={
                              (empty
                                ? 'numeric flex items-center overflow-hidden px-3 text-[12px] whitespace-nowrap text-[var(--color-ink-faint)] italic'
                                : 'numeric flex items-center overflow-hidden px-3 text-[12px] whitespace-nowrap text-[var(--color-ink)]') +
                              (onCellClick ? ' cursor-copy focus-visible:ring-2 focus-visible:ring-[var(--color-accent)]' : '') +
                              (pinned
                                ? ' sticky z-10 border-r border-[var(--color-line)] bg-[var(--color-surface)] group-hover/row:bg-[var(--color-raised)]'
                                : '')
                            }
                          >
                            <span className="truncate">{text}</span>
                          </div>
                        );
                        const startsScrolling = position === (pinnedIndex === 0 ? 1 : 0);
                        return startsScrolling && layout.leftSpacer > 0 ? [spacer(`left-${index}`), cell] : cell;
                      })}
                      {layout.rightSpacer > 0 ? spacer(`right-${index}`) : null}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
