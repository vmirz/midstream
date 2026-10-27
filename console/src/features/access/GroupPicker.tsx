import { useQuery } from '@tanstack/react-query';
import { Check } from 'lucide-react';
import type { ReactNode } from 'react';

import { BuiltInMark } from './shared';
import { Skeleton } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { useApi } from '@/lib/domains';

/**
 * Choose the groups someone belongs to. Everyone is in All Users, which is shown and fixed; a group
 * whose access you do not hold yourself cannot be given, and says why.
 */
export function GroupPicker({
  value,
  onChange,
  disabled = false,
}: {
  value: string[];
  onChange: (groupIds: string[]) => void;
  disabled?: boolean;
}): ReactNode {
  const api = useApi();
  const groups = useQuery({ queryKey: ['groups'], queryFn: () => api.groups() });
  const assignable = (groups.data?.groups ?? []).filter((group) => group.membership === 'assigned');

  if (groups.isLoading) return <Skeleton className="h-24" />;
  if (groups.error) {
    return <p className="text-[12px] text-[var(--color-phase-failed)]">Groups could not be loaded: {String((groups.error as Error).message)}</p>;
  }

  const toggle = (groupId: string): void => {
    onChange(value.includes(groupId) ? value.filter((id) => id !== groupId) : [...value, groupId]);
  };

  return (
    <div className="overflow-hidden rounded-[var(--radius-control)] border border-[var(--color-line-strong)]">
      <div className="flex items-center gap-2 border-b border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2 text-[12px] text-[var(--color-ink-muted)]">
        <Check className="size-3.5 text-[var(--color-ink-faint)]" />
        <span>
          <span className="font-medium text-[var(--color-ink)]">All Users</span> · everyone who signs in, always
        </span>
      </div>
      <ul className="max-h-60 divide-y divide-[var(--color-line)] overflow-y-auto">
        {assignable.map((group) => {
          const checked = value.includes(group.group_id);
          // Removing someone from a group you cannot grant is refused as well as adding them.
          const locked = disabled || !group.grantable;
          return (
            <li key={group.group_id}>
              <label
                className={cn(
                  'flex items-start gap-3 px-3 py-2.5 text-[13px]',
                  locked ? 'cursor-not-allowed opacity-60' : 'cursor-pointer hover:bg-[var(--color-raised)]',
                )}
                title={!group.grantable ? 'You can only give access you hold yourself' : undefined}
              >
                <input
                  type="checkbox"
                  className="mt-0.5 size-4 accent-[var(--color-accent)]"
                  checked={checked}
                  disabled={locked}
                  onChange={() => toggle(group.group_id)}
                />
                <span className="min-w-0 flex-1">
                  <span className="flex items-center gap-1.5 font-medium">
                    {group.name}
                    {group.built_in ? <BuiltInMark /> : null}
                  </span>
                  <span className="block text-[12px] text-[var(--color-ink-muted)]">
                    {group.description || 'No description'}
                    {group.role_names.length > 0 ? ` · ${group.role_names.join(', ')}` : ''}
                  </span>
                </span>
              </label>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
