import { Plus } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { CreateMaterializationDialog } from '../materializations/CreateMaterializationDialog';
import { Dialog } from '@/components/ui/Dialog';
import { Button } from '@/components/ui/primitives';
import { useConsoleAuth } from '@/lib/auth';
import { DomainProvider, useDomains, useOptionalDomain } from '@/lib/domains';
import type { DomainMaterialization } from '@/lib/fleet';
import type { ConsoleDomain } from '@shared/types';

/**
 * "New materialization": in the page's domain, or - on a page of every domain - in the domain the
 * person chooses first, of those they may create one in (handbook 20). Renders nothing for someone
 * who may create one nowhere.
 */
export function NewMaterialization({
  materializations,
  label = 'New materialization',
}: {
  materializations: readonly DomainMaterialization[] | undefined;
  label?: string;
}): ReactNode {
  const scope = useOptionalDomain();
  const auth = useConsoleAuth();
  const domains = useDomains();
  const creatable = useMemo(
    () =>
      (scope ? [scope] : domains).filter(
        (domain) => domain.status !== 'misconfigured' && auth.forDomain(domain.id).can('materializations.create'),
      ),
    [auth, domains, scope],
  );
  const [choosing, setChoosing] = useState(false);
  const [chosen, setChosen] = useState<ConsoleDomain | null>(null);

  if (creatable.length === 0) return null;
  const start = (): void => {
    if (creatable.length === 1) setChosen(creatable[0]!);
    else setChoosing(true);
  };
  return (
    <>
      <Button variant="primary" onClick={start}>
        <Plus className="size-4" />
        {label}
      </Button>
      <Dialog
        open={choosing}
        onClose={() => setChoosing(false)}
        title="Which domain?"
        description="A materialization reads topics of its domain's Kafka cluster and writes tables into its datasets."
      >
        <ul className="space-y-2">
          {creatable.map((domain) => (
            <li key={domain.id}>
              <button
                type="button"
                className="flex w-full items-center justify-between rounded-[var(--radius-control)] border border-[var(--color-line)] px-4 py-3 text-left transition-colors hover:border-[var(--color-accent)] hover:bg-[var(--color-raised)]"
                onClick={() => {
                  setChoosing(false);
                  setChosen(domain);
                }}
              >
                <span className="text-[13px] font-medium">{domain.name}</span>
                <span className="numeric text-[12px] text-[var(--color-ink-muted)]">{domain.id}</span>
              </button>
            </li>
          ))}
        </ul>
      </Dialog>
      {chosen ? (
        <DomainProvider domain={chosen}>
          <CreateMaterializationDialog
            open
            onClose={() => setChosen(null)}
            materializations={(materializations ?? []).filter((item) => item.domain === chosen.id)}
          />
        </DomainProvider>
      ) : null}
    </>
  );
}
