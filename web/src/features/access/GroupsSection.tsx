import { useQuery } from '@tanstack/react-query';
import { Plus, Search, UsersRound } from 'lucide-react';
import { useDeferredValue, useEffect, useState, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import { BuiltInMark, Chips, errorField, formatDate, tableHeaderClass, useIdentityChange } from './shared';
import { Dialog } from '@/components/ui/Dialog';
import { Button, EmptyState, Field, Input, Panel, Skeleton, Textarea } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import type { CpGroup } from '@shared/types';

const GRID = 'grid grid-cols-[minmax(0,2fr)_120px_minmax(0,1.6fr)_120px] items-center gap-4';

/** Groups bundle people so access is given once, to the group, through its roles. */
export function GroupsSection(): ReactNode {
  const auth = useAuth();
  const groups = useQuery({ queryKey: ['groups'], queryFn: () => api.groups() });
  const [search, setSearch] = useState('');
  const [creating, setCreating] = useState(false);
  const query = useDeferredValue(search.trim().toLowerCase());
  const canManage = auth.can('groups.manage');

  const rows = (groups.data?.groups ?? []).filter(
    (group) => !query || group.name.toLowerCase().includes(query) || group.description.toLowerCase().includes(query),
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[200px] flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
          <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search groups" aria-label="Search groups" className="pl-9" />
        </div>
        {canManage ? (
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus className="size-4" />
            New group
          </Button>
        ) : null}
      </div>

      <Panel className="overflow-hidden">
        <div className={cn(GRID, tableHeaderClass)}>
          <span>Group name</span>
          <span>Members</span>
          <span>Roles</span>
          <span className="text-right">Created on</span>
        </div>
        {groups.isLoading ? (
          <div className="space-y-px p-5">
            {Array.from({ length: 5 }, (_, index) => (
              <Skeleton key={index} className="h-11" />
            ))}
          </div>
        ) : groups.error ? (
          <EmptyState title="Groups could not be loaded" description={(groups.error as Error).message} />
        ) : rows.length === 0 ? (
          <EmptyState icon={<UsersRound className="size-6" />} title="No groups match" description="Try another name." />
        ) : (
          <ul className="divide-y divide-[var(--color-line)]">
            {rows.map((group) => (
              <GroupRow key={group.group_id} group={group} />
            ))}
          </ul>
        )}
      </Panel>

      {canManage ? <NewGroupDialog open={creating} onClose={() => setCreating(false)} /> : null}
    </div>
  );
}

export function memberLabel(group: Pick<CpGroup, 'membership' | 'member_count'>): string {
  if (group.membership === 'guests') return 'Not signed in';
  if (group.membership === 'authenticated') return `Everyone (${group.member_count ?? 0})`;
  return String(group.member_count ?? 0);
}

function GroupRow({ group }: { group: CpGroup }): ReactNode {
  return (
    <li>
      <Link to={`/settings/groups/${group.group_id}`} className={cn(GRID, 'px-5 py-3 transition-colors hover:bg-[var(--color-raised)]')}>
        <span className="flex min-w-0 items-center gap-2.5">
          <UsersRound className="size-4 shrink-0 text-[var(--color-accent)]" />
          <span className="min-w-0">
            <span className="flex items-center gap-1.5 text-[13px] font-medium">
              <span className="truncate">{group.name}</span>
              {group.built_in ? <BuiltInMark /> : null}
            </span>
            {group.description ? <span className="block truncate text-[12px] text-[var(--color-ink-muted)]">{group.description}</span> : null}
          </span>
        </span>
        <span className={cn('text-[13px]', group.membership === 'assigned' ? 'numeric' : 'text-[var(--color-ink-muted)]')}>
          {memberLabel(group)}
        </span>
        <Chips items={group.role_names} />
        <span className="text-right text-[12px] text-[var(--color-ink-muted)]">{formatDate(group.created_at)}</span>
      </Link>
    </li>
  );
}

function NewGroupDialog({ open, onClose }: { open: boolean; onClose: () => void }): ReactNode {
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const create = useIdentityChange(() => api.createGroup({ name: name.trim(), description: description.trim() }), {
    success: () => `${name.trim()} created`,
    failure: 'The group was not created',
    onApplied: (result) => {
      onClose();
      if (result.status === 'applied') navigate(`/settings/groups/${result.entity_id}`);
    },
  });
  const { reset } = create;
  useEffect(() => {
    if (!open) return;
    setName('');
    setDescription('');
    reset();
  }, [open, reset]);
  const error = errorField(create.error) === 'name' ? (create.error as Error).message : null;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Create a new group"
      description="Create a group, then give it roles and add people to it."
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" loading={create.isPending} disabled={!name.trim()} onClick={() => create.mutate(undefined)}>
            Create group
          </Button>
        </>
      }
    >
      <form
        className="space-y-4"
        onSubmit={(event) => {
          event.preventDefault();
          if (name.trim()) create.mutate(undefined);
        }}
      >
        <Field label="Group name" required error={error}>
          <Input autoFocus value={name} onChange={(event) => setName(event.target.value)} placeholder="Data engineering" />
        </Field>
        <Field label="Description">
          <Textarea rows={2} value={description} onChange={(event) => setDescription(event.target.value)} className="font-sans text-sm" />
        </Field>
      </form>
    </Dialog>
  );
}
