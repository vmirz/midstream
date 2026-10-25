import { Download, EllipsisVertical, Eye, Link2 } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { ExportOptions, exportRows } from './ExportOptions';
import { Popover } from '@/components/ui/Popover';
import { cn } from '@/lib/cn';
import type { ConsoleConfig, RunSnapshot } from '@shared/types';

/**
 * The Data tab's More actions, right of Run: peek at the newest records, share the view, export the result.
 *
 * Every action is always listed, so the menu does not change shape; one that does not apply is
 * disabled, with the reason in its tooltip. Export opens its options in the same menu.
 */
export function MoreActions({
  run,
  config,
  exportable,
  onShare,
  onPeek,
}: {
  run: RunSnapshot | null;
  config: ConsoleConfig | null;
  /** Whether the person may export the rows of every table the run reads. */
  exportable: boolean;
  onShare: () => void;
  /** Open the peek at the newest records; null while the table is not behind them. */
  onPeek: (() => void) | null;
}): ReactNode {
  const [view, setView] = useState<'menu' | 'export'>('menu');
  const finished = run?.state === 'succeeded';
  const rows = run && finished ? exportRows(run, config) : 0;
  const exportBlocked = !finished
    ? 'Available once the query has finished'
    : !exportable
      ? 'You do not have permission to export this data'
      : rows === 0
        ? 'Nothing to export'
        : null;

  return (
    <Popover
      align="end"
      width={view === 'export' ? 292 : 236}
      trigger={({ toggle, open, id }) => (
        <button
          type="button"
          onClick={() => {
            setView('menu');
            toggle();
          }}
          aria-expanded={open}
          aria-controls={id}
          aria-label="More actions"
          title="More actions"
          data-role="more-actions"
          className={cn(
            'inline-flex size-8 shrink-0 items-center justify-center rounded-[var(--radius-control)] transition-colors',
            'text-[var(--color-ink-muted)] hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]',
            open && 'bg-[var(--color-raised)] text-[var(--color-ink)]',
          )}
        >
          <EllipsisVertical className="size-4" />
        </button>
      )}
    >
      {({ close }) =>
        view === 'export' && run && exportBlocked === null ? (
          <ExportOptions run={run} config={config} onBack={() => setView('menu')} onDone={close} />
        ) : (
          <div className="p-1" role="menu" aria-label="More actions">
            <MenuItem
              icon={<Eye className="size-3.5" />}
              label="Peek"
              title="See the newest incoming records that are not in the table yet"
              blocked={onPeek ? null : 'Available while this table is catching up'}
              onSelect={() => {
                close();
                onPeek?.();
              }}
            />
            <MenuItem
              icon={<Link2 className="size-3.5" />}
              label="Share"
              title="Copy a link that reopens this view"
              blocked={null}
              onSelect={() => {
                close();
                onShare();
              }}
            />
            <MenuItem icon={<Download className="size-3.5" />} label="Export…" blocked={exportBlocked} onSelect={() => setView('export')} />
          </div>
        )
      }
    </Popover>
  );
}

/** One action; when it does not apply it stays listed, disabled, with the reason as its tooltip. */
function MenuItem({
  icon,
  label,
  title,
  blocked,
  onSelect,
}: {
  icon: ReactNode;
  label: string;
  title?: string;
  /** Why the action does not apply now; null when it does. */
  blocked: string | null;
  onSelect: () => void;
}): ReactNode {
  return (
    <button
      type="button"
      role="menuitem"
      aria-disabled={blocked !== null}
      title={blocked ?? title}
      onClick={blocked === null ? onSelect : undefined}
      className={cn(
        'flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-[12px] transition-colors',
        blocked === null
          ? 'text-[var(--color-ink)] hover:bg-[var(--color-raised)]'
          : 'cursor-not-allowed text-[var(--color-ink-faint)]',
      )}
    >
      <span className={cn('shrink-0', blocked === null ? 'text-[var(--color-ink-muted)]' : 'text-[var(--color-ink-faint)]')}>{icon}</span>
      {label}
    </button>
  );
}
