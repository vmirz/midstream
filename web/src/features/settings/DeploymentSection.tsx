import { Lock } from 'lucide-react';
import type { ReactNode } from 'react';

import { Panel, PanelHeader } from '@/components/ui/primitives';
import type { DeploymentSettingView, SettingValue } from '@shared/types';

import { formatNumber, humanizeBytes } from './values';

const COMPONENTS: { id: DeploymentSettingView['component']; title: string; description: string }[] = [
  {
    id: 'control-plane',
    title: 'Control plane',
    description: 'Its Kaspr app’s environment, in the control-plane manifest.',
  },
  {
    id: 'runtime',
    title: 'Runtimes',
    description: 'The runtime spec every runtime is rendered from, in the control plane’s environment.',
  },
  {
    id: 'console',
    title: 'This console',
    description: 'The console process’s environment.',
  },
];

/** Settings that only a deployment can change, shown so an administrator can see what is in force. */
export function DeploymentSection({ entries }: { entries: DeploymentSettingView[] }): ReactNode {
  return (
    <div className="space-y-5">
      {COMPONENTS.map((component) => {
        const rows = entries.filter((entry) => entry.component === component.id);
        if (rows.length === 0) return null;
        return (
          <Panel key={component.id}>
            <PanelHeader title={component.title} description={component.description} />
            <dl className="divide-y divide-[var(--color-line)]">
              {rows.map((entry) => (
                <div key={entry.key} className="grid gap-1 px-5 py-3 md:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)] md:gap-8">
                  <dt className="min-w-0">
                    <span className="flex items-center gap-1.5 text-[13px] font-medium">
                      <Lock className="size-3 text-[var(--color-ink-faint)]" aria-hidden />
                      {entry.label}
                    </span>
                    {entry.description ? (
                      <span className="mt-0.5 block text-[12px] leading-relaxed text-[var(--color-ink-muted)]">
                        {entry.description}
                      </span>
                    ) : null}
                  </dt>
                  <dd className="min-w-0 md:text-right">
                    <span className="numeric block break-all text-[13px]">{formatDeploymentValue(entry.key, entry.value)}</span>
                    {entry.envVar ? (
                      <code className="numeric mt-0.5 block break-all text-[11px] text-[var(--color-ink-faint)]">{entry.envVar}</code>
                    ) : (
                      <span className="mt-0.5 block text-[11px] text-[var(--color-ink-faint)]">Built in</span>
                    )}
                  </dd>
                </div>
              ))}
            </dl>
          </Panel>
        );
      })}
    </div>
  );
}

function formatDeploymentValue(key: string, value: SettingValue): string {
  if (value === null || value === '') return 'Not set';
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (typeof value === 'number') return /Bytes$/.test(key) ? humanizeBytes(value) : formatNumber(value);
  return value;
}
