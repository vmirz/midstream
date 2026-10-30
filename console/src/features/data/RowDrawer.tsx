import { ProtectedMark } from '../protection/ProtectionSummary';
import { fieldProtectionText } from '../protection/protection';
import { ChevronDown, ChevronUp, Copy, Filter, X } from 'lucide-react';
import { useEffect, useMemo, type ReactNode } from 'react';
import { createPortal } from 'react-dom';

import { groupColumns } from './columns';
import { readJsonShape } from './jsonShape';
import { Button } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { cn } from '@/lib/cn';
import { useMaterializationDefaults } from '@/lib/defaults';
import { formatCell, formatInstant, isInstantType, localZone } from '@/lib/format';
import { rowNester } from '@shared/fieldPaths';
import type { BrowseFilter, QueryColumn } from '@shared/types';

/**
 * Full detail for one row, including the columns hidden from the grid.
 *
 * A materialized row is wide and mostly nested paths; reading one across a horizontally scrolling
 * grid is the slowest thing in the old tab. Here every field is on one vertical axis, grouped the
 * same way as the rail, with per-field copy and filter-to-this-value.
 */
export function RowDrawer({
  row,
  columns,
  schemaOrder,
  index,
  total,
  onClose,
  onStep,
  onFilter,
}: {
  row: Record<string, unknown>;
  columns: QueryColumn[];
  schemaOrder: string[];
  index: number;
  total: number;
  onClose: () => void;
  onStep: (delta: number) => void;
  onFilter: (filter: BrowseFilter) => void;
}): ReactNode {
  const catalog = useMaterializationDefaults().defaults?.protection ?? null;
  const toast = useToast();
  const groups = useMemo(() => groupColumns(columns, schemaOrder), [columns, schemaOrder]);
  const zone = useMemo(() => localZone(), []);
  // The same layout exports use: the record rebuilt from its field paths, or one key per column.
  const nest = useMemo(
    () => rowNester(columns.map((column) => column.name), { jsonColumns: new Set(columns.filter((column) => column.json).map((column) => column.name)) }),
    [columns],
  );

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') onClose();
      if (event.key === 'ArrowDown' || event.key === 'j') onStep(1);
      if (event.key === 'ArrowUp' || event.key === 'k') onStep(-1);
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [onClose, onStep]);

  const copy = (text: string, label: string): void => {
    void navigator.clipboard.writeText(text);
    toast.success('Copied', label);
  };

  return createPortal(
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="fixed inset-0 bg-black/20 dark:bg-black/50" onClick={onClose} aria-hidden />
      <aside
        role="dialog"
        aria-modal="true"
        aria-label="Row detail"
        className="fade-in relative flex h-full w-full max-w-[520px] flex-col border-l border-[var(--color-line)] bg-[var(--color-surface)] shadow-2xl"
      >
        <header className="flex items-center gap-2 border-b border-[var(--color-line)] px-4 py-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-[13px] font-semibold tracking-tight">Row detail</h2>
            <p className="numeric mt-0.5 text-[11px] text-[var(--color-ink-muted)]">
              {index + 1} of {total} on this page
            </p>
          </div>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onStep(-1)}
            disabled={index === 0}
            aria-label="Previous row"
            className="px-1.5"
          >
            <ChevronUp className="size-4" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onStep(1)}
            disabled={index >= total - 1}
            aria-label="Next row"
            className="px-1.5"
          >
            <ChevronDown className="size-4" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            title="Copy this row as JSON, in the layout chosen under Export"
            onClick={() => {
              const nested = readJsonShape() === 'nested';
              copy(JSON.stringify(nested ? nest(row) : row, null, 2), nested ? 'Row copied as nested JSON' : 'Row copied as flat JSON');
            }}
          >
            <Copy className="size-3.5" />
            JSON
          </Button>
          <Button size="sm" variant="ghost" onClick={onClose} aria-label="Close" className="px-1.5">
            <X className="size-4" />
          </Button>
        </header>

        <div className="min-h-0 flex-1 overflow-y-auto">
          {groups.map((group) => (
            <section key={group.id}>
              <h3 className="sticky top-0 z-10 border-b border-[var(--color-line)] bg-[var(--color-raised)] px-4 py-1.5 text-[10px] font-semibold tracking-wide text-[var(--color-ink-muted)] uppercase">
                {group.label}
              </h3>
              <dl>
                {group.columns.map((column) => {
                  const value = row[column.name];
                  const empty = value === null || value === undefined;
                  // Copy and filter use the stored value; an instant is also shown in local time.
                  const text = formatCell(value);
                  const instant = !empty && typeof value === 'string' && isInstantType(column.type);
                  return (
                    <div
                      key={column.name}
                      className="group grid grid-cols-[minmax(0,180px)_minmax(0,1fr)] items-start gap-3 border-b border-[var(--color-line)] px-4 py-2 last:border-b-0 hover:bg-[var(--color-raised)]"
                    >
                      <dt
                        className="numeric flex min-w-0 items-center gap-1 pt-px text-[11px] text-[var(--color-ink-muted)]"
                        title={`${column.name} · ${column.type}${column.protection ? ` · masked: ${fieldProtectionText(column.protection, catalog)}` : ''}`}
                      >
                        <span className="truncate">{column.name}</span>
                        <ProtectedMark protection={column.protection} />
                      </dt>
                      <dd className="flex min-w-0 items-start gap-1">
                        <span
                          className={cn(
                            'numeric min-w-0 flex-1 text-[12px] break-all',
                            empty
                              ? 'text-[var(--color-ink-faint)] italic'
                              : 'text-[var(--color-ink)]',
                          )}
                        >
                          {empty ? 'null' : instant ? (
                            <>
                              {formatInstant(text)} <span className="text-[var(--color-ink-muted)]">{zone.label}</span>
                              <span className="block text-[11px] text-[var(--color-ink-muted)]">{text}</span>
                            </>
                          ) : (
                            text
                          )}
                        </span>
                        {empty ? null : (
                          <span className="flex shrink-0 gap-0.5 opacity-0 transition-opacity group-hover:opacity-100">
                            <button
                              type="button"
                              title="Filter to this value"
                              aria-label={`Filter to ${column.name}`}
                              onClick={() =>
                                onFilter({ column: column.name, operator: 'eq', value: text })
                              }
                              className="rounded p-0.5 text-[var(--color-ink-faint)] hover:text-[var(--color-ink)]"
                            >
                              <Filter className="size-3" />
                            </button>
                            <button
                              type="button"
                              title="Copy value"
                              aria-label={`Copy ${column.name}`}
                              onClick={() => copy(text, column.name)}
                              className="rounded p-0.5 text-[var(--color-ink-faint)] hover:text-[var(--color-ink)]"
                            >
                              <Copy className="size-3" />
                            </button>
                          </span>
                        )}
                      </dd>
                    </div>
                  );
                })}
              </dl>
            </section>
          ))}
        </div>
      </aside>
    </div>,
    document.body,
  );
}
