import { AlertTriangle } from 'lucide-react';
import type { ReactNode } from 'react';

import { Panel } from '@/components/ui/primitives';
import { useMaterializationDefaults } from '@/lib/defaults';

import { missingKeyNotice } from './protection';

/** The Data protection settings section's warning while the domain has no protection key. */
export function ProtectionKeyNotice(): ReactNode {
  const text = missingKeyNotice(useMaterializationDefaults().defaults?.protection);
  if (!text) return null;
  return (
    <Panel role="alert" className="flex items-start gap-2.5 px-5 py-3.5">
      <AlertTriangle className="mt-[3px] size-4 shrink-0 text-[var(--color-phase-failed)]" aria-hidden />
      <div className="space-y-0.5">
        <p className="text-[13px] font-medium">This domain has no protection key</p>
        <p className="text-[12px] text-[var(--color-ink-muted)]">{text}</p>
      </div>
    </Panel>
  );
}
