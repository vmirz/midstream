import { AlertTriangle, CheckCircle2, Radio } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { Badge, Panel, PanelHeader } from '@/components/ui/primitives';
import type { RuntimeRollout, RuntimeRolloutState } from '@shared/types';

const STATE_LABELS: Record<RuntimeRolloutState, { label: string; tone: 'running' | 'pending' | 'paused' | 'neutral'; hint: string }> = {
  current: { label: 'Current', tone: 'running', hint: 'Every member runs this revision.' },
  pending: {
    label: 'Applying',
    tone: 'pending',
    hint: 'A member still runs an older revision, or settings from before this installation; it picks these up within seconds.',
  },
  stopped: { label: 'Stopped', tone: 'neutral', hint: 'No members are running; it takes the current settings when it resumes.' },
  not_reporting: { label: 'Not reporting', tone: 'paused', hint: 'No member has reported in the last two minutes.' },
  unsupported: { label: 'Next deploy', tone: 'paused', hint: 'Its runtime predates live settings and takes them when it is next deployed.' },
};

/**
 * How far the current runtime-settings revision has reached the runtimes.
 *
 * Each runtime member reports the revision it runs on its heartbeat; the control plane counts a
 * materialization as current once every member it has heard from lately runs it. At revision 0
 * there is nothing to follow, unless a runtime still runs settings delivered before this
 * installation's (handbook 17 §17.7): those are shown until it drops them.
 */
export function RuntimeRolloutPanel({ rollout }: { rollout: RuntimeRollout | null }): ReactNode {
  if (!rollout) return null;
  const followed = rollout.revision > 0 || rollout.pending > 0;
  const running = rollout.total - rollout.stopped;
  const outstanding = rollout.materializations.filter((item) => item.state !== 'current');
  // Moving while a member still applies; once none does, any runtime left behind needs someone.
  const progress =
    running === 0
      ? { tone: 'neutral' as const, icon: null, label: 'No runtimes running' }
      : {
          tone: rollout.current === running ? ('running' as const) : rollout.pending > 0 ? ('pending' as const) : ('paused' as const),
          icon:
            rollout.current === running ? (
              <CheckCircle2 className="size-3" />
            ) : rollout.pending > 0 ? (
              <Radio className="size-3" />
            ) : (
              <AlertTriangle className="size-3" />
            ),
          label: `${rollout.current} of ${running} running current`,
        };

  return (
    <Panel>
      <PanelHeader
        title="Delivery to runtimes"
        description={
          rollout.revision > 0
            ? `Runtimes pick up revision ${rollout.revision} on their next record or heartbeat, without a restart.`
            : followed
              ? 'No runtime setting has been changed here. A runtime still running settings from before this installation drops them on its next record or heartbeat.'
              : 'No runtime setting has been changed here, so every runtime runs the values it was deployed with.'
        }
        actions={
          followed ? (
            <Badge tone={progress.tone}>
              {progress.icon}
              {progress.label}
            </Badge>
          ) : null
        }
      />
      {followed && outstanding.length > 0 ? (
        <ul className="divide-y divide-[var(--color-line)]">
          {outstanding.map((item) => {
            const state = STATE_LABELS[item.state];
            return (
              <li key={item.materializationId} className="flex items-center justify-between gap-4 px-5 py-2.5 text-[13px]">
                <Link
                  to={`/materializations/${encodeURIComponent(item.materializationId)}`}
                  className="min-w-0 truncate font-medium hover:text-[var(--color-accent)]"
                >
                  {item.name || item.materializationId}
                </Link>
                <span className="flex shrink-0 items-center gap-3 text-[12px] text-[var(--color-ink-faint)]">
                  {item.revision !== null && item.state === 'pending' ? <span className="numeric">on revision {item.revision}</span> : null}
                  <Badge tone={state.tone} title={state.hint}>
                    {state.label}
                  </Badge>
                </span>
              </li>
            );
          })}
        </ul>
      ) : null}
    </Panel>
  );
}
