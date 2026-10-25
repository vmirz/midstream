import { Table2 } from 'lucide-react';
import type { ReactNode } from 'react';

import { tableLabel } from './selectedTable';
import { Select } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import type { CpTableSpec } from '@shared/types';

/**
 * Choose one of a materialization's tables, named as SQL reads it: `<dataset>.<table>`. A
 * materialization of one topic has nothing to choose, so the picker is not shown at all. A table the
 * person may not open here - its dataset is locked to them - is listed, and cannot be chosen.
 */
export function TablePicker({
  tables,
  value,
  onChange,
  locked,
  className,
}: {
  tables: readonly CpTableSpec[];
  value: string | null;
  onChange: (name: string) => void;
  /** Whether a table cannot be chosen here. */
  locked?: (table: CpTableSpec) => boolean;
  className?: string;
}): ReactNode {
  if (tables.length < 2 || value === null) return null;
  return (
    <span className={cn('inline-flex shrink-0 items-center gap-1.5', className)}>
      <Table2 className="size-3.5 text-[var(--color-ink-muted)]" aria-hidden />
      <Select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        aria-label="Table"
        title="The table shown here; every topic of the materialization has one"
        className="h-8 w-auto max-w-[280px] text-[12px]"
      >
        {tables.map((table) => {
          const closed = locked?.(table) ?? false;
          return (
            <option key={table.name} value={table.name} disabled={closed}>
              {tableLabel(table)}
              {closed ? ' · locked' : ''}
            </option>
          );
        })}
      </Select>
    </span>
  );
}
