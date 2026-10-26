import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { Badge, Panel, PanelHeader } from '@/components/ui/primitives';
import { useCan } from '@/lib/auth';
import type { CpHeldDatasetGrant } from '@shared/types';
import { useDomainPath } from '@/lib/domains';

/**
 * Roles held on single datasets: a person's own, and their groups'; or a group's. A dataset's owners
 * grant them, on the dataset's Access tab.
 */
export function DatasetGrantsPanel({ grants, subject }: { grants: readonly CpHeldDatasetGrant[]; subject: 'user' | 'group' }): ReactNode {
  const domainPath = useDomainPath();
  const canOpen = useCan('datasets.view');
  return (
    <Panel>
      <PanelHeader
        title="Dataset access"
        description={
          subject === 'user'
            ? 'Roles on single datasets, their own and their groups’, on top of what their groups allow everywhere.'
            : 'Roles on single datasets, held by everyone in the group.'
        }
      />
      {grants.length === 0 ? (
        <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">No roles on single datasets.</p>
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {grants.map((grant) => (
            <li key={`${grant.dataset_id}:${grant.principal_type}:${grant.principal_id}`} className="flex flex-wrap items-center justify-between gap-3 px-5 py-2.5 text-[13px]">
              <span className="min-w-0">
                {canOpen ? (
                  <Link to={domainPath(`/datasets/${encodeURIComponent(grant.dataset)}/access`)} className="numeric font-medium hover:text-[var(--color-accent)] hover:underline">
                    {grant.dataset}
                  </Link>
                ) : (
                  <span className="numeric font-medium">{grant.dataset}</span>
                )}
                {grant.via_group ? <span className="ml-2 text-[12px] text-[var(--color-ink-muted)]">through {grant.via_group}</span> : null}
              </span>
              <span className="flex flex-wrap gap-1">
                {grant.roles.map((role) => (
                  <Badge key={role.role_id} tone={role.role_id === 'dataset-owner' ? 'accent' : 'neutral'}>
                    {role.name}
                  </Badge>
                ))}
              </span>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
