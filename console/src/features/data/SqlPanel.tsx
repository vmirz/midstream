import { Copy, ListFilter, Repeat } from 'lucide-react';
import { Suspense, lazy, type ReactNode } from 'react';

import { Button, Skeleton } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { cn } from '@/lib/cn';
import type { CpTableSpec, StatementPosition } from '@shared/types';

// CodeMirror is only needed once someone writes SQL, so it stays out of the initial bundle.
const QueryEditor = lazy(() =>
  import('./QueryEditor').then((module) => ({ default: module.QueryEditor })),
);

/**
 * The statement editor, for questions filters cannot ask.
 *
 * Every table a statement reads is named in full, `<domain>.<dataset>.<table>` (handbook 20): the
 * materialization's own, and any other the person may query, in any domain. Each is bounded by the
 * time range picker, exactly as the structured view is, so a statement is as cheap
 * as the range is narrow without having to say so in its own WHERE; a statement joins them by name.
 * Run is the tab's one Run control; the editor adds only its keyboard shortcut. With live tail on,
 * Auto re-run runs the statement again a few seconds after each run finishes, so its answer follows
 * the newest records; a statement is never refreshed in place.
 */
export function SqlPanel({
  sql,
  tableName,
  domain,
  tables,
  otherTables = [],
  tableColumns,
  rangeLabel,
  columns,
  error,
  onChange,
  onRun,
  onExit,
  autoRerun = null,
}: {
  sql: string;
  /** The table on screen: what the statement was started from. */
  tableName: string;
  /** The domain the materialization is in: the first part of its tables' names. */
  domain: string;
  /** Every table of the materialization the person may query, which a statement may read by name. */
  tables: readonly CpTableSpec[];
  /** Tables of other materializations, in any domain, a statement may also name: offered as it is written. */
  otherTables?: ReadonlyArray<{ domain: string; dataset: string; name: string }>;
  /** Each table's column names, for completion. */
  tableColumns: Record<string, string[]>;
  rangeLabel: string;
  /** The columns of the table on screen. */
  columns: string[];
  error: { message: string; position?: StatementPosition | null } | null;
  onChange: (value: string) => void;
  onRun: () => void;
  onExit: () => void;
  /** The Auto re-run toggle; null where it does not apply (no live tail). */
  autoRerun?: { on: boolean; onChange: (on: boolean) => void } | null;
}): ReactNode {
  const toast = useToast();
  const current = tables.find((table) => table.name === tableName);
  const dataset = current?.dataset ?? 'dataset';

  return (
    <div className="flex h-full min-h-0 flex-col bg-[var(--color-surface)]">
      <div className="flex shrink-0 flex-wrap items-center gap-2 px-3 py-1.5">
        <span className="text-[10px] font-semibold tracking-wide text-[var(--color-ink-faint)] uppercase">SQL</span>
        <span className="min-w-0 truncate text-[11px] text-[var(--color-ink-muted)]">
          {tables.length > 1 ? (
            <>
              {tables.map((table, index) => (
                <span
                  key={table.name}
                  title={`The rows of topic ${table.source_topic}; its native columns are ${domain}_iceberg.${table.dataset}.${table.name}`}
                >
                  {index > 0 ? ', ' : ''}
                  <code className="numeric">
                    {domain}.{table.dataset}.{table.name}
                  </code>
                </span>
              ))}{' '}
              hold the rows in
            </>
          ) : (
            <>
              <code className="numeric" title={`Its native columns are ${domain}_iceberg.${dataset}.${tableName}`}>
                {domain}.{dataset}.{tableName}
              </code>{' '}
              holds the rows in
            </>
          )}{' '}
          <span className="font-medium text-[var(--color-ink)]">{rangeLabel}</span>
        </span>
        <span className="ml-auto flex items-center gap-1">
          {autoRerun ? (
            <button
              type="button"
              aria-pressed={autoRerun.on}
              onClick={() => autoRerun.onChange(!autoRerun.on)}
              title={
                autoRerun.on
                  ? 'Stop running the statement again'
                  : 'Run the statement again 5 seconds after each run finishes, so its answer follows the newest records'
              }
              className={cn(
                'inline-flex h-7 items-center gap-1 rounded-[var(--radius-control)] px-2 text-[11px] transition-colors',
                autoRerun.on
                  ? 'bg-[var(--color-accent-soft)] font-medium text-[var(--color-accent)]'
                  : 'text-[var(--color-ink-muted)] hover:bg-[var(--color-raised)]',
              )}
              data-role="auto-rerun"
            >
              <Repeat className="size-3.5" />
              Auto re-run
            </button>
          ) : null}
          <Button
            size="sm"
            variant="ghost"
            className="h-7 px-2 text-[11px]"
            onClick={() => {
              void navigator.clipboard.writeText(sql);
              toast.success('Copied', 'Statement copied to the clipboard.');
            }}
          >
            <Copy className="size-3.5" />
            Copy
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="h-7 px-2 text-[11px]"
            onClick={onExit}
            title="Leave SQL and go back to filtering the table"
          >
            <ListFilter className="size-3.5" />
            Back to filters
          </Button>
        </span>
      </div>

      {/* `min-h-0` lets the editor shrink with the divider rather than overflow it. */}
      <div className="flex min-h-0 flex-1 flex-col px-3 pb-3">
        <Suspense fallback={<Skeleton className="min-h-0 flex-1" />}>
          <QueryEditor
            value={sql}
            tableName={tableName}
            domain={domain}
            tables={tables}
            otherTables={otherTables}
            tableColumns={tableColumns}
            columns={columns}
            onChange={onChange}
            onRun={onRun}
            error={error}
          />
        </Suspense>
      </div>
    </div>
  );
}
