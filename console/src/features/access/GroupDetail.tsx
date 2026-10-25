import { useQuery } from '@tanstack/react-query';
import { Search, Trash2, UserMinus, UserPlus, UsersRound } from 'lucide-react';
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import { ConfirmDialog } from './ConfirmDialog';
import { DatasetGrantsPanel } from './DatasetGrantsPanel';
import { memberLabel } from './GroupsSection';
import { BackLink, BuiltInMark, errorField, permissionMap, useCatalog, useIdentityChange } from './shared';
import { Avatar } from '@/components/Avatar';
import { RelativeTime } from '@/components/Inline';
import { Dialog } from '@/components/ui/Dialog';
import { Badge, Button, EmptyState, Field, Input, Panel, PanelHeader, Skeleton, Textarea } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import type { CpGroupDetail } from '@shared/types';

/** One group: the roles it gives, the people in it, and what that adds up to. */
export function GroupDetail({ groupId }: { groupId: string }): ReactNode {
  const auth = useAuth();
  const group = useQuery({ queryKey: ['group', groupId], queryFn: () => api.group(groupId) });
  const detail = group.data;

  if (group.isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-16" />
        <Skeleton className="h-48" />
      </div>
    );
  }
  if (group.error || !detail) {
    return (
      <div className="space-y-4">
        <BackLink to="/settings/groups">All groups</BackLink>
        <Panel>
          <EmptyState title="This group could not be loaded" description={group.error instanceof Error ? group.error.message : 'It may have been deleted.'} />
        </Panel>
      </div>
    );
  }

  const editable = auth.can('groups.manage') && detail.manageable;
  return (
    <div className="space-y-5">
      <BackLink to="/settings/groups">All groups</BackLink>
      <div className="flex items-start gap-3">
        <span className="grid size-10 shrink-0 place-items-center rounded-full bg-[var(--color-accent-soft)] text-[var(--color-accent)]">
          <UsersRound className="size-5" />
        </span>
        <div className="min-w-0">
          <h2 className="flex items-center gap-2 text-[17px] font-semibold tracking-tight">
            {detail.name}
            {detail.built_in ? <BuiltInMark /> : null}
          </h2>
          <p className="text-[13px] text-[var(--color-ink-muted)]">{detail.description || 'No description'}</p>
          <p className="mt-1 text-[12px] text-[var(--color-ink-faint)]">
            {detail.membership === 'guests'
              ? 'Everyone who has not signed in. Signed-in people keep what guests can do.'
              : detail.membership === 'authenticated'
                ? 'Everyone who signs in belongs to this group, automatically.'
                : `${memberLabel(detail)} ${detail.member_count === 1 ? 'member' : 'members'}`}
          </p>
        </div>
      </div>
      {auth.can('groups.manage') && !detail.manageable ? (
        <p className="text-[12px] text-[var(--color-ink-muted)]">This group grants access you do not hold, so you cannot change it.</p>
      ) : null}

      <RolesPanel group={detail} editable={editable} />
      {detail.membership === 'assigned' ? <MembersPanel group={detail} editable={editable} /> : null}
      <GroupAccessPanel group={detail} />
      <DatasetGrantsPanel grants={detail.dataset_grants ?? []} subject="group" />
      {!detail.built_in && editable ? <GroupSettingsPanel group={detail} /> : null}
    </div>
  );
}

function RolesPanel({ group, editable }: { group: CpGroupDetail; editable: boolean }): ReactNode {
  const roles = useQuery({ queryKey: ['roles'], queryFn: () => api.roles(), enabled: useAuth().can('roles.view') });
  const [draft, setDraft] = useState<string[]>(group.role_ids);
  useEffect(() => setDraft(group.role_ids), [group.role_ids]);
  const save = useIdentityChange(() => api.updateGroup(group.group_id, { role_ids: draft, base_revision: group.revision }), {
    success: () => 'Roles saved',
    failure: 'The roles were not saved',
  });
  const changed = draft.length !== group.role_ids.length || draft.some((id) => !group.role_ids.includes(id));
  const locked = new Set(group.locked_role_ids);

  return (
    <Panel>
      <PanelHeader title="Roles" description="Everyone in the group holds every permission of these roles." />
      {editable && roles.data ? (
        <ul className="divide-y divide-[var(--color-line)]">
          {roles.data.roles.map((role) => {
            const checked = draft.includes(role.role_id);
            return (
              <li key={role.role_id}>
                <label className={cn('flex items-start gap-3 px-5 py-3 text-[13px]', locked.has(role.role_id) ? 'cursor-not-allowed' : 'cursor-pointer hover:bg-[var(--color-raised)]')}>
                  <input
                    type="checkbox"
                    className="mt-0.5 size-4 accent-[var(--color-accent)]"
                    checked={checked}
                    disabled={locked.has(role.role_id)}
                    onChange={() => setDraft(checked ? draft.filter((id) => id !== role.role_id) : [...draft, role.role_id])}
                  />
                  <span className="min-w-0 flex-1">
                    <span className="flex items-center gap-1.5 font-medium">
                      {role.name}
                      {role.built_in ? <Badge tone="neutral">Built in</Badge> : null}
                      {locked.has(role.role_id) ? <span className="text-[12px] font-normal text-[var(--color-ink-faint)]">always held by this group</span> : null}
                    </span>
                    <span className="block text-[12px] text-[var(--color-ink-muted)]">{role.description}</span>
                  </span>
                </label>
              </li>
            );
          })}
        </ul>
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {group.roles.length === 0 ? <li className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">No roles.</li> : null}
          {group.roles.map((role) => (
            <li key={role.role_id} className="px-5 py-3 text-[13px]">
              <Link to={`/settings/roles/${role.role_id}`} className="font-medium hover:underline">
                {role.name}
              </Link>
            </li>
          ))}
        </ul>
      )}
      {editable ? (
        <div className="flex justify-end gap-2 border-t border-[var(--color-line)] px-5 py-3">
          <Button size="sm" variant="ghost" disabled={!changed} onClick={() => setDraft(group.role_ids)}>
            Undo
          </Button>
          <Button size="sm" variant="primary" disabled={!changed} loading={save.isPending} onClick={() => save.mutate(undefined)}>
            Save roles
          </Button>
        </div>
      ) : null}
    </Panel>
  );
}

function MembersPanel({ group, editable }: { group: CpGroupDetail; editable: boolean }): ReactNode {
  const [adding, setAdding] = useState(false);
  const remove = useIdentityChange((userId: string) => api.updateGroup(group.group_id, { remove_member_ids: [userId] }), {
    success: () => 'Removed from the group',
    failure: 'They were not removed',
  });
  const members = group.members;

  return (
    <Panel>
      <PanelHeader
        title="Members"
        description="People added to this group."
        actions={
          editable ? (
            <Button size="sm" onClick={() => setAdding(true)}>
              <UserPlus className="size-3.5" />
              Add people
            </Button>
          ) : null
        }
      />
      {members === null ? (
        <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">Seeing the members needs the “View users page with emails” permission.</p>
      ) : members.length === 0 ? (
        <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">No one is in this group yet.</p>
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {members.map((member) => (
            <li key={member.user_id} className="flex items-center justify-between gap-3 px-5 py-2.5">
              <Link to={`/settings/users/${member.user_id}`} className="flex min-w-0 items-center gap-3">
                <Avatar name={member.display_name} email={member.email} muted={member.status === 'deactivated'} />
                <span className="min-w-0">
                  <span className="block truncate text-[13px] font-medium">{member.display_name}</span>
                  <span className="block truncate text-[12px] text-[var(--color-ink-muted)]">{member.email}</span>
                </span>
                {member.status === 'deactivated' ? <Badge tone="neutral">Deactivated</Badge> : null}
              </Link>
              <span className="flex items-center gap-3">
                <span className="hidden text-[12px] text-[var(--color-ink-muted)] sm:inline">
                  Active <RelativeTime value={member.last_active_at} />
                </span>
                {editable ? (
                  <Button size="sm" variant="ghost" disabled={remove.isPending} onClick={() => remove.mutate(member.user_id)} aria-label={`Remove ${member.display_name}`}>
                    <UserMinus className="size-3.5" />
                  </Button>
                ) : null}
              </span>
            </li>
          ))}
        </ul>
      )}
      {editable ? <AddMembersDialog open={adding} onClose={() => setAdding(false)} group={group} /> : null}
    </Panel>
  );
}

function AddMembersDialog({ open, onClose, group }: { open: boolean; onClose: () => void; group: CpGroupDetail }): ReactNode {
  const users = useQuery({ queryKey: ['users', 'active', 'name', ''], queryFn: () => api.users({ status: 'active' }), enabled: open });
  const [search, setSearch] = useState('');
  const [chosen, setChosen] = useState<string[]>([]);
  useEffect(() => {
    if (!open) return;
    setSearch('');
    setChosen([]);
  }, [open]);
  const memberIds = useMemo(() => new Set((group.members ?? []).map((member) => member.user_id)), [group.members]);
  const candidates = (users.data?.users ?? []).filter(
    (user) =>
      !memberIds.has(user.user_id) &&
      (!search || `${user.display_name} ${user.email}`.toLowerCase().includes(search.trim().toLowerCase())),
  );
  const add = useIdentityChange(() => api.updateGroup(group.group_id, { add_member_ids: chosen }), {
    success: () => (chosen.length === 1 ? 'Added to the group' : `${chosen.length} people added`),
    failure: 'They were not added',
    onApplied: () => onClose(),
  });

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Add people to ${group.name}`}
      description="They get every role of the group at their next request."
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" disabled={chosen.length === 0} loading={add.isPending} onClick={() => add.mutate(undefined)}>
            Add {chosen.length > 0 ? chosen.length : ''} {chosen.length === 1 ? 'person' : 'people'}
          </Button>
        </>
      }
    >
      <div className="space-y-3">
        <div className="relative">
          <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
          <Input autoFocus value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search by name or email" className="pl-9" />
        </div>
        <ul className="max-h-72 divide-y divide-[var(--color-line)] overflow-y-auto rounded-[var(--radius-control)] border border-[var(--color-line-strong)]">
          {users.isLoading ? <li className="p-3"><Skeleton className="h-8" /></li> : null}
          {!users.isLoading && candidates.length === 0 ? (
            <li className="px-3 py-4 text-center text-[13px] text-[var(--color-ink-muted)]">No one else to add.</li>
          ) : null}
          {candidates.map((user) => {
            const checked = chosen.includes(user.user_id);
            return (
              <li key={user.user_id}>
                <label className="flex cursor-pointer items-center gap-3 px-3 py-2 hover:bg-[var(--color-raised)]">
                  <input
                    type="checkbox"
                    className="size-4 accent-[var(--color-accent)]"
                    checked={checked}
                    onChange={() => setChosen(checked ? chosen.filter((id) => id !== user.user_id) : [...chosen, user.user_id])}
                  />
                  <Avatar name={user.display_name} email={user.email} size="sm" />
                  <span className="min-w-0">
                    <span className="block truncate text-[13px] font-medium">{user.display_name}</span>
                    <span className="block truncate text-[12px] text-[var(--color-ink-muted)]">{user.email}</span>
                  </span>
                </label>
              </li>
            );
          })}
        </ul>
      </div>
    </Dialog>
  );
}

function GroupAccessPanel({ group }: { group: CpGroupDetail }): ReactNode {
  const catalog = useCatalog();
  const byKey = permissionMap(catalog.data);
  return (
    <Panel>
      <PanelHeader
        title="Access"
        description={group.all_permissions ? 'Every permission, including ones added later.' : 'Every permission the group’s roles add up to.'}
      />
      {group.permissions.length === 0 ? (
        <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">This group grants no permissions yet.</p>
      ) : (
        <div className="flex flex-wrap gap-1.5 px-5 py-4">
          {group.permissions.map((key) => (
            <span
              key={key}
              title={byKey.get(key)?.description}
              className="rounded-md bg-[var(--color-raised)] px-2 py-1 text-[12px] text-[var(--color-ink-muted)] ring-1 ring-[var(--color-line)] ring-inset"
            >
              {byKey.get(key)?.label ?? key}
            </span>
          ))}
        </div>
      )}
    </Panel>
  );
}

function GroupSettingsPanel({ group }: { group: CpGroupDetail }): ReactNode {
  const navigate = useNavigate();
  const [name, setName] = useState(group.name);
  const [description, setDescription] = useState(group.description);
  const [confirmDelete, setConfirmDelete] = useState(false);
  useEffect(() => {
    setName(group.name);
    setDescription(group.description);
  }, [group.name, group.description]);
  const save = useIdentityChange(
    () => api.updateGroup(group.group_id, { name: name.trim(), description: description.trim(), base_revision: group.revision }),
    { success: () => 'Group saved', failure: 'The group was not saved' },
  );
  const remove = useIdentityChange(() => api.deleteGroup(group.group_id), {
    success: () => `${group.name} deleted`,
    failure: 'The group was not deleted',
    onApplied: () => navigate('/settings/groups'),
  });
  const changed = name.trim() !== group.name || description.trim() !== group.description;
  const nameError = errorField(save.error) === 'name' ? (save.error as Error).message : null;

  return (
    <Panel>
      <PanelHeader title="Settings" />
      <div className="space-y-4 px-5 py-4">
        <Field label="Group name" error={nameError}>
          <Input value={name} onChange={(event) => setName(event.target.value)} />
        </Field>
        <Field label="Description">
          <Textarea rows={2} value={description} onChange={(event) => setDescription(event.target.value)} className="font-sans text-sm" />
        </Field>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2 border-t border-[var(--color-line)] px-5 py-3">
        <Button size="sm" variant="ghost" onClick={() => setConfirmDelete(true)}>
          <Trash2 className="size-3.5" />
          Delete group
        </Button>
        <Button size="sm" variant="primary" disabled={!changed || !name.trim()} loading={save.isPending} onClick={() => save.mutate(undefined)}>
          Save
        </Button>
      </div>
      <ConfirmDialog
        open={confirmDelete}
        title={`Delete ${group.name}?`}
        description={`Its ${group.member_count ?? 0} ${group.member_count === 1 ? 'member loses' : 'members lose'} what it granted, at their next request.`}
        confirmLabel="Delete group"
        danger
        pending={remove.isPending}
        onConfirm={() => remove.mutate(undefined)}
        onClose={() => setConfirmDelete(false)}
      />
    </Panel>
  );
}
