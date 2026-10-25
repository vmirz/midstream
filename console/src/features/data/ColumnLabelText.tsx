import { type ReactNode } from 'react';

import { labelFor, type ColumnLabel } from './columns';
import { cn } from '@/lib/cn';

/**
 * Render a column name so the part that identifies it always survives.
 *
 * The parent path shrinks and truncates; the leaf segment never does. `caused_by.id` degrades to
 * `…id`, not to `caused_by…`, which is what makes a rail of deep paths readable at 250px.
 */
export function ColumnLabelText({
  labels,
  name,
  className,
}: {
  labels: Map<string, ColumnLabel>;
  name: string;
  className?: string;
}): ReactNode {
  const label = labelFor(labels, name);
  return (
    <span className={cn('flex min-w-0 items-baseline', className)} title={label.full}>
      {label.prefix ? (
        <span className="truncate text-[var(--color-ink-faint)]">{label.prefix}</span>
      ) : null}
      <span className="shrink-0">{label.leaf}</span>
    </span>
  );
}
