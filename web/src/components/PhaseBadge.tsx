import { useMemo, type ReactNode } from 'react';

import { Badge } from './ui/primitives';
import { cn } from '@/lib/cn';
import { humanize } from '@/lib/format';
import type { LifecyclePhase, RuntimeHealthLevel } from '@shared/types';

type Tone = 'neutral' | 'running' | 'pending' | 'paused' | 'failed';

const PHASE_TONES: Record<LifecyclePhase, Tone> = {
  requested: 'pending',
  discovering: 'pending',
  discovered: 'pending',
  deploying: 'pending',
  running: 'running',
  paused: 'paused',
  stopped: 'paused',
  deleting: 'paused',
  deleted: 'neutral',
  failed: 'failed',
  degraded: 'failed',
};

const TONE_DOT: Record<Tone, string> = {
  neutral: 'bg-[var(--color-phase-idle)]',
  running: 'bg-[var(--color-phase-running)]',
  pending: 'bg-[var(--color-phase-pending)]',
  paused: 'bg-[var(--color-phase-paused)]',
  failed: 'bg-[var(--color-phase-failed)]',
};

export function phaseTone(phase: LifecyclePhase): Tone {
  return PHASE_TONES[phase] ?? 'neutral';
}

export function PhaseBadge({
  phase,
  reason,
  className,
}: {
  phase: LifecyclePhase;
  reason?: string | null;
  className?: string;
}): ReactNode {
  const tone = phaseTone(phase);
  return (
    <Badge tone={tone} className={className} title={reason ? humanize(reason) : undefined}>
      <span
        className={cn(
          'size-1.5 rounded-full',
          TONE_DOT[tone],
          (phase === 'running' || tone === 'pending') && 'live-dot',
        )}
      />
      {humanize(phase)}
    </Badge>
  );
}

export function PhaseDot({ phase, className }: { phase: LifecyclePhase; className?: string }): ReactNode {
  return <span className={cn('inline-block size-2 rounded-full', TONE_DOT[phaseTone(phase)], className)} />;
}

const HEALTH_TONE: Record<RuntimeHealthLevel, Tone> = {
  healthy: 'running',
  degraded: 'paused',
  unknown: 'neutral',
  unreachable: 'failed',
};

/** A degraded runtime is said to be unhealthy: "Degraded" is the phase badge's word beside it. */
const HEALTH_LABEL: Record<RuntimeHealthLevel, string> = {
  healthy: 'Healthy',
  degraded: 'Unhealthy',
  unknown: 'Unknown',
  unreachable: 'Unreachable',
};

export function HealthBadge({
  level,
  reason,
}: {
  level: RuntimeHealthLevel;
  /** Why, for the tooltip: the health's message, in words for a person, or its reason. */
  reason?: string | null;
}): ReactNode {
  return (
    <Badge tone={HEALTH_TONE[level]} title={reason ?? undefined}>
      <span className={cn('size-1.5 rounded-full', TONE_DOT[HEALTH_TONE[level]])} />
      {HEALTH_LABEL[level] ?? humanize(level)}
    </Badge>
  );
}

/** Compact desired→observed reconciliation indicator. */
export function DriftIndicator({
  desired,
  observed,
}: {
  desired: LifecyclePhase;
  observed: LifecyclePhase;
}): ReactNode {
  const converged = useMemo(() => desired === observed, [desired, observed]);
  if (converged) return null;
  return (
    <Badge tone="pending" className="gap-1">
      <span className="live-dot size-1.5 rounded-full bg-[var(--color-phase-pending)]" />
      Reconciling → {humanize(desired)}
    </Badge>
  );
}
