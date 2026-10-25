import { useQuery } from '@tanstack/react-query';
import { Search, UserPlus, Users } from 'lucide-react';
import { useDeferredValue, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { AddUserDialog } from './AddUserDialog';
import { Chips, UserStatusBadges, tableHeaderClass } from './shared';
import { Avatar } from '@/components/Avatar';
import { RelativeTime } from '@/components/Inline';
import { Button, EmptyState, Input, Panel, Select, Skeleton } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import type { CpUser, UserFilter, UserSort } from '@shared/types';

const FILTER_LABELS: Record<UserFilter, string> = {
  all: 'All users',
  active: 'Active',
  deactivated: 'Deactivated',
  locked: 'Locked out',
  password_change: 'Password to change',
};

const SORT_LABELS: Record<UserSort, string> = {
  name: 'Sort by name',
  last_active: 'Sort by last active',
  created: 'Sort by date added',
};

const GRID = 'grid grid-cols-[minmax(0,2fr)_minmax(0,1.6fr)_minmax(0,1fr)_120px] items-center gap-4';

/** Everyone who can sign in, like a team directory: who they are, their groups, how active they are. */
export function UsersSection(): ReactNode {
  const auth = useAuth();
  const [filter, setFilter] = useState<UserFilter>('active');
  const [sort, setSort] = useState<UserSort>('name');
  const [search, setSearch] = useState('');
  const [adding, setAdding] = useState(false);
  const query = useDeferredValue(search.trim());

  const users = useQuery({
    queryKey: ['users', filter, sort, query],
    queryFn: () => api.users({ status: filter, sort, q: query || undefined }),
    placeholderData: (previous) => previous,
  });
  const counts = users.data?.counts;
  const canManage = auth.can('users.manage');
  const canOpen = auth.can('users.view_details');

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <Select
          aria-label="Show"
          value={filter}
          onChange={(event) => setFilter(event.target.value as UserFilter)}
          className="w-auto min-w-[190px]"
        >
          {(Object.keys(FILTER_LABELS) as UserFilter[]).map((key) => (
            <option key={key} value={key}>
              {FILTER_LABELS[key]}
              {counts ? ` (${counts[key]})` : ''}
            </option>
          ))}
        </Select>
        <Select aria-label="Sort" value={sort} onChange={(event) => setSort(event.target.value as UserSort)} className="w-auto">
          {(Object.keys(SORT_LABELS) as UserSort[]).map((key) => (
            <option key={key} value={key}>
              {SORT_LABELS[key]}
            </option>
          ))}
        </Select>
        <div className="relative min-w-[200px] flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search for a user"
            aria-label="Search for a user"
            className="pl-9"
            spellCheck={false}
          />
        </div>
        {canManage ? (
          <Button variant="primary" onClick={() => setAdding(true)}>
            <UserPlus className="size-4" />
            Add user
          </Button>
        ) : null}
      </div>

      <p className="text-[12px] text-[var(--color-ink-muted)]">
        {users.data
          ? `Showing ${users.data.returned} ${filter === 'all' ? '' : `${FILTER_LABELS[filter].toLowerCase()} `}${users.data.returned === 1 ? 'user' : 'users'}.`
          : ' '}
      </p>

      <Panel className="overflow-hidden">
        <div className={cn(GRID, tableHeaderClass)}>
          <span>Name</span>
          <span>Groups</span>
          <span>Status</span>
          <span className="text-right">Last active</span>
        </div>
        {users.isLoading ? (
          <div className="space-y-px p-5">
            {Array.from({ length: 5 }, (_, index) => (
              <Skeleton key={index} className="h-12" />
            ))}
          </div>
        ) : users.error ? (
          <EmptyState
            title="Users could not be loaded"
            description={users.error instanceof Error ? users.error.message : String(users.error)}
            action={<Button onClick={() => void users.refetch()}>Retry</Button>}
          />
        ) : (users.data?.users.length ?? 0) === 0 ? (
          <EmptyState
            icon={<Users className="size-6" />}
            title={search ? 'No one matches' : filter === 'active' ? 'No active users' : `No ${FILTER_LABELS[filter].toLowerCase()} users`}
            description={search ? 'Try another name or email.' : 'People an administrator adds appear here.'}
            action={
              canManage && !search ? (
                <Button variant="primary" onClick={() => setAdding(true)}>
                  <UserPlus className="size-4" />
                  Add user
                </Button>
              ) : null
            }
          />
        ) : (
          <ul className="divide-y divide-[var(--color-line)]">
            {users.data!.users.map((user) => (
              <UserRow key={user.user_id} user={user} link={canOpen} />
            ))}
          </ul>
        )}
      </Panel>

      {canManage ? <AddUserDialog open={adding} onClose={() => setAdding(false)} /> : null}
    </div>
  );
}

function UserRow({ user, link }: { user: CpUser; link: boolean }): ReactNode {
  const content = (
    <div className={cn(GRID, 'px-5 py-3', link && 'transition-colors hover:bg-[var(--color-raised)]')}>
      <span className="flex min-w-0 items-center gap-3">
        <Avatar name={user.display_name} email={user.email} muted={user.status === 'deactivated'} />
        <span className="min-w-0">
          <span className={cn('block truncate text-[13px] font-medium', user.status === 'deactivated' && 'text-[var(--color-ink-muted)]')}>
            {user.display_name}
          </span>
          <span className="block truncate text-[12px] text-[var(--color-ink-muted)]">{user.email}</span>
        </span>
      </span>
      <Chips items={['All Users', ...user.groups.map((group) => group.name)]} />
      <span className="flex min-w-0 flex-wrap items-center gap-1">
        {user.status === 'active' && !user.locked_until && !user.must_change_password ? (
          <span className="text-[12px] text-[var(--color-ink-muted)]">Active</span>
        ) : null}
        <UserStatusBadges user={{ ...user, administrator: false, self: false }} />
      </span>
      <RelativeTime value={user.last_active_at} className="text-right text-[12px] text-[var(--color-ink-muted)]" />
    </div>
  );
  return <li>{link ? <Link to={`/settings/users/${user.user_id}`}>{content}</Link> : content}</li>;
}
