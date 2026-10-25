import { useId, type ReactNode } from 'react';

import { cn } from '@/lib/cn';

export interface SparklinePoint {
  t: number;
  value: number | null;
}

/**
 * Dependency-free area sparkline.
 *
 * Charts here are small, numerous, and updated every poll tick, so a hand-rolled SVG keeps the
 * bundle light and the render cost per update near zero.
 */
export function Sparkline({
  points,
  className,
  height = 40,
  strokeWidth = 1.5,
  color = 'var(--color-accent)',
  showArea = true,
}: {
  points: SparklinePoint[];
  className?: string;
  height?: number;
  strokeWidth?: number;
  color?: string;
  showArea?: boolean;
}): ReactNode {
  const gradientId = useId();
  const values = points.map((point) => point.value).filter((value): value is number => value !== null);

  if (values.length < 2) {
    return (
      <div
        className={cn('flex items-center justify-center text-[11px] text-[var(--color-ink-faint)]', className)}
        style={{ height }}
      >
        Collecting…
      </div>
    );
  }

  const width = 240;
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const span = max - min || 1;
  const step = width / (values.length - 1);

  const coordinates = values.map((value, index) => {
    const x = index * step;
    const y = height - ((value - min) / span) * (height - strokeWidth * 2) - strokeWidth;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  });

  const line = `M ${coordinates.join(' L ')}`;
  const area = `${line} L ${width},${height} L 0,${height} Z`;

  return (
    <svg
      className={cn('w-full', className)}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      aria-hidden
    >
      {showArea ? (
        <>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.18" />
              <stop offset="100%" stopColor={color} stopOpacity="0" />
            </linearGradient>
          </defs>
          <path d={area} fill={`url(#${gradientId})`} />
        </>
      ) : null}
      <path
        d={line}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        strokeLinejoin="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}

/** Horizontal proportion bar used for partition and phase distributions. */
export function DistributionBar({
  segments,
  className,
}: {
  segments: { label: string; value: number; color: string }[];
  className?: string;
}): ReactNode {
  const total = segments.reduce((sum, segment) => sum + segment.value, 0);
  if (total === 0) {
    return <div className={cn('h-1.5 rounded-full bg-[var(--color-raised)]', className)} />;
  }
  return (
    <div className={cn('flex h-1.5 gap-0.5 overflow-hidden rounded-full', className)}>
      {segments
        .filter((segment) => segment.value > 0)
        .map((segment) => (
          <div
            key={segment.label}
            title={`${segment.label}: ${segment.value}`}
            style={{ width: `${(segment.value / total) * 100}%`, backgroundColor: segment.color }}
          />
        ))}
    </div>
  );
}
