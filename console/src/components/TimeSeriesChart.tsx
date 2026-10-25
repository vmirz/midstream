import { useEffect, useId, useMemo, useRef, useState, type ReactNode, type RefObject } from 'react';

import { cn } from '@/lib/cn';
import { formatRelativeTime } from '@/lib/format';
import { timeTicks, valueAxis } from '@/lib/ticks';

export interface ChartPoint {
  /** Epoch milliseconds. */
  t: number;
  value: number | null;
}

interface Valued {
  t: number;
  value: number;
}

const MARGIN = { top: 10, right: 16, bottom: 22 };
/** Width of one character of the 11px monospace axis labels. */
const CHAR_PX = 6.6;

const pad = (value: number): string => String(value).padStart(2, '0');

/** `14:35`, or `14:35:30` when ticks are closer than a minute: local, 24-hour, fixed width. */
function clockLabel(t: number, seconds: boolean): string {
  const date = new Date(t);
  const base = `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  return seconds ? `${base}:${pad(date.getSeconds())}` : base;
}

/** The element's size, kept current as it resizes. Zero until it has been measured. */
function useElementSize(ref: RefObject<HTMLElement | null>): { width: number; height: number } {
  const [size, setSize] = useState({ width: 0, height: 0 });
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    const box = element.getBoundingClientRect();
    setSize({ width: box.width, height: box.height });
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (rect) setSize((current) => (current.width === rect.width && current.height === rect.height ? current : { width: rect.width, height: rect.height }));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);
  return size;
}

/**
 * One series over a window of time, with axes and a hover readout.
 *
 * Where `Sparkline` is a glyph that says "rising" or "flat", this is for reading the series: the
 * value axis has round ticks, the time axis has clock times, and pointing at — or arrowing along —
 * the line reads out a point's value and when it was measured. Rendered at the container's real
 * width, so text and markers are never stretched. Gaps in the series (nulls) break the line rather
 * than being drawn through.
 */
export function TimeSeriesChart({
  points,
  from,
  to,
  height: fixedHeight,
  color = 'var(--color-accent)',
  baseline = 'zero',
  formatValue,
  formatTick = formatValue,
  label,
  empty = 'Collecting…',
  className,
}: {
  points: ChartPoint[];
  /** The window shown, epoch milliseconds. */
  from: number;
  to: number;
  /**
   * Height of the plot including its time axis. Without one the chart fills its container, which
   * must then be given a height — a flex child that grows, say, with a minimum.
   */
  height?: number;
  color?: string;
  /** `zero` keeps zero on the axis (rates); `auto` fits the axis to the values when they stay well clear of it. */
  baseline?: 'zero' | 'auto';
  formatValue: (value: number) => string;
  formatTick?: (value: number) => string;
  /** What the series is, for the accessible name and the readout. */
  label: string;
  /** Shown instead of a chart with fewer than two values in the window. */
  empty?: ReactNode;
  className?: string;
}): ReactNode {
  const containerRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(containerRef);
  const width = size.width;
  const height = fixedHeight ?? Math.max(120, size.height);
  // A fixed height is set on the element; a filling chart takes its container's.
  const boxStyle = fixedHeight === undefined ? { height: '100%' } : { height: fixedHeight };
  const gradientId = useId();
  const clipId = useId();
  const [hover, setHover] = useState<number | null>(null);

  // Points in the window, plus the last one before it, so the line enters from the left edge.
  const visible = useMemo(() => {
    const sorted = points.filter((point) => Number.isFinite(point.t)).sort((a, b) => a.t - b.t);
    const firstInside = sorted.findIndex((point) => point.t >= from);
    if (firstInside === -1) return [];
    return sorted.slice(Math.max(0, firstInside - 1)).filter((point) => point.t <= to);
  }, [points, from, to]);
  const valued = useMemo(
    () => visible.filter((point): point is Valued => point.value !== null && Number.isFinite(point.value)),
    [visible],
  );
  const inside = useMemo(() => valued.filter((point) => point.t >= from), [valued, from]);

  const axis = useMemo(() => valueAxis(inside.map((point) => point.value), { baseline }), [inside, baseline]);
  const left = Math.ceil(Math.max(...axis.ticks.map((tick) => formatTick(tick).length), 1) * CHAR_PX) + 12;
  const plotWidth = Math.max(1, width - left - MARGIN.right);
  const plotHeight = Math.max(1, height - MARGIN.top - MARGIN.bottom);
  const span = Math.max(1, to - from);
  const x = (t: number): number => left + ((t - from) / span) * plotWidth;
  const y = (value: number): number =>
    MARGIN.top + (1 - (value - axis.min) / Math.max(axis.max - axis.min, Number.EPSILON)) * plotHeight;

  const time = useMemo(() => timeTicks(from, to, Math.max(2, Math.floor(plotWidth / 90))), [from, to, plotWidth]);

  // One path per unbroken run of values; a null ends a run.
  const runs = useMemo(() => {
    const result: Valued[][] = [];
    let current: Valued[] = [];
    for (const point of visible) {
      if (point.value === null || !Number.isFinite(point.value)) {
        if (current.length > 0) result.push(current);
        current = [];
      } else {
        current.push(point as Valued);
      }
    }
    if (current.length > 0) result.push(current);
    return result;
  }, [visible]);

  const active = hover === null ? null : (valued[Math.min(hover, valued.length - 1)] ?? null);
  const last = valued.at(-1) ?? null;

  const pick = (clientX: number): void => {
    const box = containerRef.current?.getBoundingClientRect();
    if (!box || valued.length === 0) return;
    const t = from + ((clientX - box.left - left) / plotWidth) * span;
    let best = 0;
    for (let index = 1; index < valued.length; index += 1) {
      if (Math.abs((valued[index] as Valued).t - t) < Math.abs((valued[best] as Valued).t - t)) best = index;
    }
    setHover(best);
  };

  // One element whether or not there is a chart yet, so the size observer stays on it.
  if (inside.length < 2) {
    return (
      <div ref={containerRef} className={cn('relative', className)} style={boxStyle}>
        <div className="flex h-full items-center justify-center text-[11px] text-[var(--color-ink-faint)]">{empty}</div>
      </div>
    );
  }

  const baselineY = y(Math.max(axis.min, Math.min(0, axis.max)));
  const line = (run: Valued[]): string => `M ${run.map((point) => `${x(point.t).toFixed(1)},${y(point.value).toFixed(1)}`).join(' L ')}`;
  const area = (run: Valued[]): string =>
    `${line(run)} L ${x((run.at(-1) as Valued).t).toFixed(1)},${baselineY.toFixed(1)} L ${x((run[0] as Valued).t).toFixed(1)},${baselineY.toFixed(1)} Z`;
  const tooltipLeft = active ? x(active.t) : 0;
  const flip = tooltipLeft > width - 170;
  // The readout sits in the half of the plot away from the point it describes, so it never covers
  // the line it is reading.
  const tooltipBelow = active !== null && y(active.value) < MARGIN.top + plotHeight / 2;

  return (
    <div
      ref={containerRef}
      className={cn('relative outline-none focus-visible:rounded-[var(--radius-control)] focus-visible:ring-2 focus-visible:ring-[var(--color-accent-soft)]', className)}
      style={boxStyle}
      tabIndex={0}
      role="group"
      aria-label={`${label}: ${last ? formatValue(last.value) : 'no data'}. Use the left and right arrow keys to read earlier values.`}
      onPointerMove={(event) => pick(event.clientX)}
      onPointerLeave={() => setHover(null)}
      onFocus={() => setHover((current) => current ?? valued.length - 1)}
      onBlur={() => setHover(null)}
      onKeyDown={(event) => {
        const current = hover ?? valued.length - 1;
        if (event.key === 'ArrowLeft') setHover(Math.max(0, current - 1));
        else if (event.key === 'ArrowRight') setHover(Math.min(valued.length - 1, current + 1));
        else if (event.key === 'Home') setHover(0);
        else if (event.key === 'End') setHover(valued.length - 1);
        else if (event.key === 'Escape') setHover(null);
        else return;
        event.preventDefault();
      }}
    >
      {width > 0 ? (
        <svg width={width} height={height} className="block overflow-visible" aria-hidden>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.14" />
              <stop offset="100%" stopColor={color} stopOpacity="0.02" />
            </linearGradient>
            <clipPath id={clipId}>
              <rect x={left} y={MARGIN.top - 4} width={plotWidth} height={plotHeight + 8} />
            </clipPath>
          </defs>

          {axis.ticks.map((tick) => (
            <g key={tick}>
              <line x1={left} x2={left + plotWidth} y1={y(tick)} y2={y(tick)} stroke="var(--color-line)" strokeWidth={1} shapeRendering="crispEdges" />
              <text x={left - 8} y={y(tick)} dy="0.32em" textAnchor="end" className="numeric" fontSize={11} fill="var(--color-ink-faint)">
                {formatTick(tick)}
              </text>
            </g>
          ))}
          {time.ticks.map((tick) => (
            <text key={tick} x={x(tick)} y={height - 6} textAnchor="middle" className="numeric" fontSize={11} fill="var(--color-ink-faint)">
              {clockLabel(tick, time.step < 60_000)}
            </text>
          ))}

          <g clipPath={`url(#${clipId})`}>
            {runs.map((run) => (
              <path key={`area-${run[0]?.t}`} d={area(run)} fill={`url(#${gradientId})`} />
            ))}
            {runs.map((run) => (
              <path
                key={`line-${run[0]?.t}`}
                d={line(run)}
                fill="none"
                stroke={color}
                strokeWidth={2}
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            ))}
          </g>

          {last && !active ? (
            <circle cx={x(last.t)} cy={y(last.value)} r={3.5} fill={color} stroke="var(--color-surface)" strokeWidth={2} />
          ) : null}
          {active ? (
            <>
              <line
                x1={x(active.t)}
                x2={x(active.t)}
                y1={MARGIN.top}
                y2={MARGIN.top + plotHeight}
                stroke="var(--color-ink-faint)"
                strokeWidth={1}
                shapeRendering="crispEdges"
              />
              <circle cx={x(active.t)} cy={y(active.value)} r={4} fill={color} stroke="var(--color-surface)" strokeWidth={2} />
            </>
          ) : null}
        </svg>
      ) : null}

      {active ? (
        <div
          role="status"
          className="pointer-events-none absolute z-10 rounded-[var(--radius-control)] border border-[var(--color-line-strong)] bg-[var(--color-surface)] px-2.5 py-1.5 shadow-lg shadow-black/10"
          style={{
            ...(flip ? { right: Math.max(4, width - tooltipLeft + 10) } : { left: tooltipLeft + 10 }),
            ...(tooltipBelow ? { bottom: MARGIN.bottom + 6 } : { top: 4 }),
          }}
        >
          <p className="numeric text-[13px] font-semibold whitespace-nowrap text-[var(--color-ink)]">{formatValue(active.value)}</p>
          <p className="mt-0.5 flex items-center gap-1.5 text-[11px] whitespace-nowrap text-[var(--color-ink-muted)]">
            <span aria-hidden className="inline-block h-0.5 w-3 rounded-full" style={{ backgroundColor: color }} />
            {label} · <span className="numeric">{clockLabel(active.t, true)}</span> · {formatRelativeTime(new Date(active.t).toISOString())}
          </p>
        </div>
      ) : null}
    </div>
  );
}
