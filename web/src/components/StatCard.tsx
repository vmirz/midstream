import type { ReactNode } from 'react';

import { Sparkline, type SparklinePoint } from './Sparkline';
import { cn } from '@/lib/cn';

export function StatCard({
  label,
  value,
  unit,
  hint,
  series,
  color,
  className,
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  hint?: ReactNode;
  series?: SparklinePoint[];
  color?: string;
  className?: string;
}): ReactNode {
  return (
    <div className={cn('panel flex flex-col overflow-hidden', className)}>
      <div className="px-4 pt-3.5 pb-3">
        <p className="text-[11px] font-medium tracking-wide text-[var(--color-ink-faint)] uppercase">
          {label}
        </p>
        <p className="mt-1.5 flex items-baseline gap-1">
          <span className="numeric text-[22px] leading-none font-semibold tracking-tight">{value}</span>
          {unit ? <span className="text-[12px] text-[var(--color-ink-muted)]">{unit}</span> : null}
        </p>
        {hint ? <div className="mt-1.5 text-[12px] text-[var(--color-ink-muted)]">{hint}</div> : null}
      </div>
      {series ? (
        <div className="mt-auto">
          <Sparkline points={series} height={44} color={color} />
        </div>
      ) : null}
    </div>
  );
}
