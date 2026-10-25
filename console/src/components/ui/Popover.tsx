import { useEffect, useId, useRef, useState, type ReactNode } from 'react';

import { cn } from '@/lib/cn';

/**
 * Lightweight anchored popover.
 *
 * Rendered inline rather than through a portal: every use site sits inside the explorer toolbar,
 * so inline keeps the panel inside the same stacking and scroll context as its trigger and avoids
 * having to track the anchor's position on scroll.
 */
export function Popover({
  trigger,
  children,
  align = 'start',
  side = 'bottom',
  width = 320,
  className,
}: {
  trigger: (props: { open: boolean; toggle: () => void; id: string }) => ReactNode;
  children: (props: { close: () => void }) => ReactNode;
  align?: 'start' | 'end';
  /** Which side of the trigger the panel opens on; `top` for triggers near the bottom of the page. */
  side?: 'bottom' | 'top';
  width?: number;
  className?: string;
}): ReactNode {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const id = useId();

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent): void => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('pointerdown', onPointerDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open]);

  return (
    <div ref={containerRef} className="relative">
      {trigger({ open, toggle: () => setOpen((value) => !value), id })}
      {open ? (
        <div
          id={id}
          role="dialog"
          style={{ width }}
          className={cn(
            'fade-in absolute z-40 max-w-[calc(100vw-2rem)]',
            side === 'top' ? 'bottom-[calc(100%+6px)]' : 'top-[calc(100%+6px)]',
            'rounded-[var(--radius-panel)] border border-[var(--color-line-strong)]',
            'bg-[var(--color-surface)] shadow-xl shadow-black/10 dark:shadow-black/50',
            align === 'end' ? 'right-0' : 'left-0',
            className,
          )}
        >
          {children({ close: () => setOpen(false) })}
        </div>
      ) : null}
    </div>
  );
}
