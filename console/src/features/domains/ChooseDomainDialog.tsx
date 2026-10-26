import type { ReactNode } from 'react';

import { DomainDot } from '@/components/DomainChip';
import { Dialog } from '@/components/ui/Dialog';
import type { ConsoleDomain } from '@shared/types';

/**
 * Which domain something new goes in, asked first on a page of every domain (handbook 20): a
 * materialization or a dataset belongs to exactly one.
 */
export function ChooseDomainDialog({
  open,
  onClose,
  domains,
  onChoose,
  title = 'Which domain?',
  description,
}: {
  open: boolean;
  onClose: () => void;
  /** The domains it may go in: those where the person may create it. */
  domains: readonly ConsoleDomain[];
  onChoose: (domain: ConsoleDomain) => void;
  title?: string;
  description?: string;
}): ReactNode {
  return (
    <Dialog open={open} onClose={onClose} title={title} description={description}>
      <ul className="space-y-2">
        {domains.map((domain) => (
          <li key={domain.id}>
            <button
              type="button"
              className="flex w-full items-center gap-3 rounded-[var(--radius-control)] border border-[var(--color-line)] px-4 py-3 text-left transition-colors hover:border-[var(--color-accent)] hover:bg-[var(--color-raised)]"
              onClick={() => {
                onClose();
                onChoose(domain);
              }}
            >
              <DomainDot domain={domain} />
              <span className="flex-1 text-[13px] font-medium">{domain.name}</span>
              <span className="numeric text-[12px] text-[var(--color-ink-muted)]">{domain.id}</span>
            </button>
          </li>
        ))}
      </ul>
    </Dialog>
  );
}
