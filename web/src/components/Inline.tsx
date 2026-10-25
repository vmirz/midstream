import { Check, Copy } from 'lucide-react';
import { useEffect, useState, type ReactNode } from 'react';

import { cn } from '@/lib/cn';
import { formatRelativeTime, formatTimestamp } from '@/lib/format';
import { useTicker } from '@/lib/live';

export function CopyButton({
  value,
  label,
  className,
}: {
  value: string;
  label?: string;
  className?: string;
}): ReactNode {
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const timer = setTimeout(() => setCopied(false), 1400);
    return () => clearTimeout(timer);
  }, [copied]);

  return (
    <button
      type="button"
      onClick={() => {
        void navigator.clipboard.writeText(value).then(() => setCopied(true));
      }}
      aria-label={label ?? `Copy ${value}`}
      title={label ?? 'Copy'}
      className={cn(
        'inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[11px] text-[var(--color-ink-faint)]',
        'transition-colors hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]',
        className,
      )}
    >
      {copied ? <Check className="size-3" /> : <Copy className="size-3" />}
    </button>
  );
}

/** Relative timestamp that stays current without triggering a data refetch. */
export function RelativeTime({
  value,
  className,
  prefix,
}: {
  value: string | null | undefined;
  className?: string;
  prefix?: string;
}): ReactNode {
  const now = useTicker(1000);
  if (!value) return <span className={cn('text-[var(--color-ink-faint)]', className)}>—</span>;
  return (
    <time dateTime={value} title={formatTimestamp(value)} className={className}>
      {prefix ? `${prefix} ` : ''}
      {formatRelativeTime(value, now)}
    </time>
  );
}

/** Monospace identifier with an inline copy affordance. */
export function Identifier({
  value,
  className,
  truncate = false,
}: {
  value: string;
  className?: string;
  truncate?: boolean;
}): ReactNode {
  return (
    <span className={cn('group inline-flex min-w-0 items-center gap-1', className)}>
      <code className={cn('numeric text-[12px] text-[var(--color-ink-muted)]', truncate && 'truncate')}>
        {value}
      </code>
      <CopyButton value={value} className="opacity-0 transition-opacity group-hover:opacity-100" />
    </span>
  );
}
