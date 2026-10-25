import { useQuery } from '@tanstack/react-query';
import { Lock, Plus, Search, ShieldCheck } from 'lucide-react';
import { useDeferredValue, useState, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import { tableHeaderClass } from './shared';
import { Badge, Button, EmptyState, Input, Panel, Skeleton } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';

const GRID = 'grid grid-cols-[minmax(0,1.3fr)_minmax(0,2fr)_110px_100px] items-center gap-4';

/** Roles: reusable sets of permissions, given to groups. */
export function RolesSection(): ReactNode {
  const auth = useAuth();
  const navigate = useNavigate();
  const roles = useQuery({ queryKey: ['roles'], queryFn: () => api.roles() });
  const [search, setSearch] = useState('');
  const query = useDeferredValue(search.trim().toLowerCase());
  const rows = (roles.data?.roles ?? []).filter(
    (role) => !query || role.name.toLowerCase().includes(query) || role.description.toLowerCase().includes(query),
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[200px] flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
          <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search roles" aria-label="Search roles" className="pl-9" />
        </div>
        {auth.can('roles.manage') ? (
          <Button variant="primary" onClick={() => navigate('/settings/roles/new')}>
            <Plus className="size-4" />
            Create role
          </Button>
        ) : null}
      </div>

      <Panel className="overflow-hidden">
        <div className={cn(GRID, tableHeaderClass)}>
          <span>Role</span>
          <span>Description</span>
          <span>Type</span>
          <span className="text-right">Groups</span>
        </div>
        {roles.isLoading ? (
          <div className="space-y-px p-5">
            {Array.from({ length: 5 }, (_, index) => (
              <Skeleton key={index} className="h-11" />
            ))}
          </div>
        ) : roles.error ? (
          <EmptyState title="Roles could not be loaded" description={(roles.error as Error).message} />
        ) : rows.length === 0 ? (
          <EmptyState icon={<ShieldCheck className="size-6" />} title="No roles match" description="Try another name." />
        ) : (
          <ul className="divide-y divide-[var(--color-line)]">
            {rows.map((role) => (
              <li key={role.role_id}>
                <Link to={`/settings/roles/${role.role_id}`} className={cn(GRID, 'px-5 py-3 transition-colors hover:bg-[var(--color-raised)]')}>
                  <span className="flex min-w-0 items-center gap-2.5">
                    <ShieldCheck className="size-4 shrink-0 text-[var(--color-accent)]" />
                    <span className="truncate text-[13px] font-medium">{role.name}</span>
                  </span>
                  <span className="truncate text-[13px] text-[var(--color-ink-muted)]" title={role.description}>
                    {role.description || '—'}
                  </span>
                  <span>
                    {role.built_in ? (
                      <Badge tone="neutral">
                        <Lock className="size-3" />
                        Built in
                      </Badge>
                    ) : (
                      <Badge tone="accent">Custom</Badge>
                    )}
                  </span>
                  <span className="text-right text-[13px] text-[var(--color-ink-muted)]">
                    {role.groups.length} {role.groups.length === 1 ? 'group' : 'groups'}
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </div>
  );
}
