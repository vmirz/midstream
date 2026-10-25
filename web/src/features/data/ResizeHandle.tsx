import { useCallback, useEffect, useRef, type ReactNode } from 'react';

import { cn } from '@/lib/cn';

export type ResizeOrientation = 'horizontal' | 'vertical';

/**
 * Drag bar between two panes.
 *
 * Which pane deserves the screen depends entirely on what someone is doing — composing a long
 * statement, reading a wide result, or reading a column path deep enough to truncate — and that
 * changes minute to minute, so it is a decision the layout should let them make rather than one to
 * guess at.
 *
 * `orientation` describes the bar itself, as `aria-orientation` does: a `horizontal` bar lies
 * across the layout and is dragged up and down, a `vertical` one stands between two columns and is
 * dragged left and right. `onResize` always receives the delta along the axis the bar moves.
 *
 * Pointer capture means the drag survives the cursor leaving the 12px bar, which is otherwise the
 * usual way a resize feels like it slips. Arrow keys move it too: a divider that only responds to
 * a precise drag is unusable by anyone not using a mouse.
 */
export function ResizeHandle({
  onResize,
  value,
  min,
  max,
  label,
  orientation = 'horizontal',
}: {
  /** Called with the pointer's delta in pixels, along the bar's axis of movement. */
  onResize: (delta: number) => void;
  /** Current size of the pane before the bar, for assistive technology. */
  value: number;
  min: number;
  max: number;
  label: string;
  orientation?: ResizeOrientation;
}): ReactNode {
  const vertical = orientation === 'vertical';
  const dragging = useRef(false);
  const last = useRef(0);

  const stop = useCallback(() => {
    dragging.current = false;
    document.body.style.removeProperty('cursor');
    document.body.style.removeProperty('user-select');
  }, []);

  useEffect(() => stop, [stop]);

  /** The coordinate this bar moves along. */
  const coordinate = (event: { clientX: number; clientY: number }): number =>
    vertical ? event.clientX : event.clientY;

  return (
    <div
      role="separator"
      aria-orientation={orientation}
      aria-label={label}
      aria-valuenow={Math.round(value)}
      aria-valuemin={min}
      aria-valuemax={max}
      tabIndex={0}
      onPointerDown={(event) => {
        dragging.current = true;
        last.current = coordinate(event);
        event.currentTarget.setPointerCapture(event.pointerId);
        // Held on the body so the cursor and the text-selection block persist across the whole
        // drag, not only while the pointer happens to be over the bar.
        document.body.style.cursor = vertical ? 'col-resize' : 'row-resize';
        document.body.style.userSelect = 'none';
      }}
      onPointerMove={(event) => {
        if (!dragging.current) return;
        const position = coordinate(event);
        const delta = position - last.current;
        last.current = position;
        if (delta !== 0) onResize(delta);
      }}
      onPointerUp={(event) => {
        event.currentTarget.releasePointerCapture(event.pointerId);
        stop();
      }}
      onPointerCancel={stop}
      onKeyDown={(event) => {
        const step = event.shiftKey ? 48 : 12;
        const back = vertical ? 'ArrowLeft' : 'ArrowUp';
        const forward = vertical ? 'ArrowRight' : 'ArrowDown';
        if (event.key === back) {
          event.preventDefault();
          onResize(-step);
        } else if (event.key === forward) {
          event.preventDefault();
          onResize(step);
        }
      }}
      className={cn(
        'group relative flex shrink-0 items-center justify-center',
        'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-accent)]',
        vertical ? 'h-full w-3 cursor-col-resize' : 'h-3 w-full cursor-row-resize',
      )}
    >
      {/*
        Always drawn, quietly. Hover-only was a control nobody could find: an affordance that
        appears once you are already pointing at it only helps people who guessed it was there.
        It is faint at rest and firms up under the pointer, which is enough to read as draggable
        without becoming a line across the page.
      */}
      <span
        className={cn(
          'rounded-full transition-all',
          'bg-[var(--color-line-strong)] opacity-60',
          'group-hover:bg-[var(--color-accent)] group-hover:opacity-100',
          'group-focus-visible:bg-[var(--color-accent)] group-focus-visible:opacity-100',
          vertical
            ? 'h-12 w-[3px] group-hover:h-16 group-focus-visible:h-16'
            : 'h-[3px] w-12 group-hover:w-16 group-focus-visible:w-16',
        )}
      />
    </div>
  );
}
