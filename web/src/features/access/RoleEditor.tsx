import { useQuery } from '@tanstack/react-query';
import { ChevronDown, Copy, Lock, Search, Trash2 } from 'lucide-react';
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';

import { ConfirmDialog } from './ConfirmDialog';
import { BackLink, errorField, expandPermissions, permissionMap, useCatalog, useIdentityChange } from './shared';
import { Badge, Button, EmptyState, Field, Input, Panel, Select, Skeleton, Textarea } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import type { CpPermissionCatalog, CpPermissionDefinition, CpRole } from '@shared/types';

/**
 * Create a role, or see and change one: its name, its description, and its permissions, chosen by
 * category. A permission that another chosen one implies is shown included, since the role grants
 * it either way. Each permission says where it holds: across the deployment, or on a dataset - held
 * through a group, a dataset permission holds on every dataset; a role of dataset permissions only
 * can also be granted on one dataset. Built-in roles are shown read-only, and can be duplicated into
 * a custom one.
 */
export function RoleEditor({ roleId }: { roleId: string }): ReactNode {
  const creating = roleId === 'new';
  const [searchParams] = useSearchParams();
  const sourceId = creating ? searchParams.get('from') : roleId;
  const role = useQuery({ queryKey: ['role', sourceId], queryFn: () => api.role(sourceId!), enabled: Boolean(sourceId) });
  const catalog = useCatalog();

  if ((sourceId && role.isLoading) || catalog.isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-12" />
        <Skeleton className="h-64" />
      </div>
    );
  }
  if ((sourceId && (role.error || !role.data)) || catalog.error || !catalog.data) {
    return (
      <div className="space-y-4">
        <BackLink to="/settings/roles">All roles</BackLink>
        <Panel>
          <EmptyState
            title="This role could not be loaded"
            description={((role.error ?? catalog.error) as Error | null)?.message ?? 'It may have been deleted.'}
          />
        </Panel>
      </div>
    );
  }
  return <RoleForm key={`${roleId}:${role.data?.revision ?? 0}`} creating={creating} role={role.data ?? null} catalog={catalog.data} />;
}

function RoleForm({ creating, role, catalog }: { creating: boolean; role: CpRole | null; catalog: CpPermissionCatalog }): ReactNode {
  const auth = useAuth();
  const navigate = useNavigate();
  const readOnly = !creating && (role?.built_in || !auth.can('roles.manage'));
  const original = useMemo(
    () => ({
      name: creating ? (role ? `${role.name} copy` : '') : (role?.name ?? ''),
      description: role?.description ?? '',
      permissions: role?.all_permissions ? catalog.permissions.map((permission) => permission.key) : (role?.permissions ?? []),
    }),
    [catalog.permissions, creating, role],
  );
  const [name, setName] = useState(original.name);
  const [description, setDescription] = useState(original.description);
  const [chosen, setChosen] = useState<Set<string>>(() => new Set(original.permissions));
  const [search, setSearch] = useState('');
  const [category, setCategory] = useState('all');
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [confirmDelete, setConfirmDelete] = useState(false);

  useEffect(() => {
    setName(original.name);
    setDescription(original.description);
    setChosen(new Set(original.permissions));
  }, [original]);

  const byKey = permissionMap(catalog);
  const effective = useMemo(() => expandPermissions(chosen, catalog), [catalog, chosen]);
  // Who includes a permission the person did not tick themselves.
  const includedBy = useMemo(() => {
    const included = new Map<string, string>();
    for (const key of chosen) {
      for (const implied of expandPermissions([key], catalog)) {
        if (implied !== key && !chosen.has(implied) && !included.has(implied)) included.set(implied, byKey.get(key)?.label ?? key);
      }
    }
    return included;
  }, [byKey, catalog, chosen]);

  const save = useIdentityChange(
    () =>
      creating
        ? api.createRole({ name: name.trim(), description: description.trim(), permissions: [...chosen] })
        : api.updateRole(role!.role_id, {
            name: name.trim(),
            description: description.trim(),
            permissions: [...chosen],
            base_revision: role!.revision,
          }),
    {
      success: () => (creating ? `${name.trim()} created` : 'Role saved'),
      failure: creating ? 'The role was not created' : 'The role was not saved',
      onApplied: (result) => {
        if (creating && result.status === 'applied') navigate(`/settings/roles/${result.entity_id}`, { replace: true });
      },
    },
  );
  const remove = useIdentityChange(() => api.deleteRole(role!.role_id), {
    success: () => `${role?.name} deleted`,
    failure: 'The role was not deleted',
    onApplied: () => navigate('/settings/roles'),
  });

  const changed =
    name.trim() !== original.name ||
    description.trim() !== original.description ||
    chosen.size !== original.permissions.length ||
    original.permissions.some((key) => !chosen.has(key));
  const nameError = errorField(save.error) === 'name' ? (save.error as Error).message : !name.trim() && changed ? 'Name is required' : null;
  const permissionError = errorField(save.error) === 'permissions' ? (save.error as Error).message : null;

  const visible = (definition: CpPermissionDefinition): boolean => {
    if (category !== 'all' && definition.category !== category) return false;
    const text = search.trim().toLowerCase();
    return !text || definition.label.toLowerCase().includes(text) || definition.description.toLowerCase().includes(text) || definition.key.includes(text);
  };
  const toggle = (key: string): void => {
    setChosen((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };
  const setCategoryChosen = (categoryId: string, value: boolean): void => {
    setChosen((current) => {
      const next = new Set(current);
      for (const definition of catalog.permissions) {
        if (definition.category !== categoryId || !visible(definition)) continue;
        if (value) next.add(definition.key);
        else next.delete(definition.key);
      }
      return next;
    });
  };

  const title = creating ? 'Create role' : (role?.name ?? 'Role');

  return (
    <div className="space-y-5">
      <BackLink to="/settings/roles">All roles</BackLink>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-[17px] font-semibold tracking-tight">{title}</h2>
        <div className="flex gap-2">
          {!creating && auth.can('roles.manage') ? (
            <Button onClick={() => navigate(`/settings/roles/new?from=${encodeURIComponent(role!.role_id)}`)}>
              <Copy className="size-3.5" />
              Duplicate
            </Button>
          ) : null}
          {!readOnly ? (
            <Button variant="primary" disabled={!changed || !name.trim()} loading={save.isPending} onClick={() => save.mutate(undefined)}>
              {creating ? 'Create role' : 'Save changes'}
            </Button>
          ) : null}
        </div>
      </div>
      {role?.built_in && !creating ? (
        <div className="flex items-start gap-2 rounded-[var(--radius-panel)] border border-[var(--color-line)] bg-[var(--color-raised)] px-4 py-3 text-[13px]">
          <Lock className="mt-0.5 size-4 shrink-0 text-[var(--color-ink-faint)]" />
          <span>
            Built-in roles cannot be changed: they gain new permissions as the application does. Duplicate it to make a role of your own.
          </span>
        </div>
      ) : null}

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_240px]">
        <div className="min-w-0 space-y-5">
          <Panel className="space-y-4 px-5 py-4">
            <p className="text-[13px] font-semibold tracking-tight">Role details</p>
            <Field label="Name" required error={nameError}>
              <Input value={name} disabled={readOnly} onChange={(event) => setName(event.target.value)} placeholder="Enter a name" />
            </Field>
            <Field label="Description">
              <Textarea
                rows={2}
                value={description}
                disabled={readOnly}
                onChange={(event) => setDescription(event.target.value)}
                placeholder="Provide a description for this role"
                className="font-sans text-sm"
              />
            </Field>
          </Panel>

          <div className="space-y-3">
            <p className="text-[13px] font-semibold tracking-tight">Permissions</p>
            {permissionError ? <p className="text-[12px] text-[var(--color-phase-failed)]">{permissionError}</p> : null}
            <div className="flex flex-wrap gap-2">
              <div className="relative min-w-[200px] flex-1">
                <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
                <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search for permissions" className="pl-9" />
              </div>
              <Select value={category} onChange={(event) => setCategory(event.target.value)} className="w-auto" aria-label="Permission type">
                <option value="all">All permission types</option>
                {catalog.categories.map((item) => (
                  <option key={item.id} value={item.id}>
                    {item.label}
                  </option>
                ))}
              </Select>
            </div>
            {catalog.categories.map((item) => {
              const rows = catalog.permissions.filter((definition) => definition.category === item.id && visible(definition));
              if (rows.length === 0) return null;
              const isCollapsed = collapsed.has(item.id);
              return (
                <Panel key={item.id} className="overflow-hidden">
                  <div className="flex items-center justify-between gap-3 border-b border-[var(--color-line)] bg-[var(--color-raised)] px-4 py-2.5">
                    <button
                      type="button"
                      aria-expanded={!isCollapsed}
                      onClick={() =>
                        setCollapsed((current) => {
                          const next = new Set(current);
                          if (next.has(item.id)) next.delete(item.id);
                          else next.add(item.id);
                          return next;
                        })
                      }
                      className="flex items-center gap-2 text-[13px] font-medium"
                    >
                      <ChevronDown className={cn('size-4 transition-transform', isCollapsed && '-rotate-90')} />
                      {item.label}
                    </button>
                    {!readOnly ? (
                      <span className="flex items-center gap-2 text-[12px]">
                        <button type="button" className="text-[var(--color-accent)] hover:underline" onClick={() => setCategoryChosen(item.id, true)}>
                          Select all
                        </button>
                        <span className="text-[var(--color-line-strong)]">|</span>
                        <button type="button" className="text-[var(--color-accent)] hover:underline" onClick={() => setCategoryChosen(item.id, false)}>
                          Deselect all
                        </button>
                      </span>
                    ) : null}
                  </div>
                  {isCollapsed ? null : (
                    <ul className="divide-y divide-[var(--color-line)]">
                      {rows.map((definition) => {
                        const checked = chosen.has(definition.key);
                        const implied = includedBy.get(definition.key);
                        return (
                          <li key={definition.key}>
                            <label className={cn('flex items-start gap-3 px-4 py-2.5 text-[13px]', readOnly ? 'cursor-default' : 'cursor-pointer hover:bg-[var(--color-raised)]')}>
                              <input
                                type="checkbox"
                                className="mt-0.5 size-4 accent-[var(--color-accent)]"
                                checked={checked || Boolean(implied)}
                                disabled={readOnly || Boolean(implied && !checked)}
                                onChange={() => toggle(definition.key)}
                              />
                              <span className="min-w-0 flex-1">
                                <span className="flex flex-wrap items-center gap-1.5">
                                  {definition.label}
                                  {definition.scope === 'dataset' ? (
                                    <Badge title="Held through a group, on every dataset; granted on a dataset, on that one only">Per dataset</Badge>
                                  ) : null}
                                  {definition.danger ? <Badge tone="failed">Can remove data or access</Badge> : null}
                                  {implied && !checked ? <span className="text-[12px] text-[var(--color-ink-faint)]">included by {implied}</span> : null}
                                </span>
                                <span className="block text-[12px] text-[var(--color-ink-muted)]">{definition.description}</span>
                              </span>
                            </label>
                          </li>
                        );
                      })}
                    </ul>
                  )}
                </Panel>
              );
            })}
          </div>

          {!creating && role && !role.built_in && auth.can('roles.manage') ? (
            <Panel className="flex flex-wrap items-center justify-between gap-3 px-5 py-4">
              <span className="text-[13px]">
                <span className="font-medium">Delete this role</span>
                <span className="block text-[12px] text-[var(--color-ink-muted)]">
                  {role.groups.length > 0
                    ? `It is removed from ${role.groups.length} ${role.groups.length === 1 ? 'group' : 'groups'}, whose members lose what it granted.`
                    : 'No group holds it.'}
                </span>
              </span>
              <Button variant="ghost" onClick={() => setConfirmDelete(true)}>
                <Trash2 className="size-3.5" />
                Delete role
              </Button>
            </Panel>
          ) : null}
        </div>

        <aside className="space-y-5 lg:sticky lg:top-6 lg:self-start">
          <div>
            <p className="text-[12px] font-medium text-[var(--color-ink-muted)]">Role type</p>
            <p className="mt-1 text-[13px]">{role?.built_in && !creating ? 'Built in' : 'Custom'}</p>
            {effective.size > 0 && [...effective].every((key) => catalog.permissions.find((definition) => definition.key === key)?.scope === 'dataset') ? (
              <p className="mt-1 text-[12px] text-[var(--color-ink-muted)]">
                Only dataset permissions: it can be granted on a single dataset, from that dataset’s Access tab.
              </p>
            ) : null}
          </div>
          <div>
            <p className="text-[12px] font-medium text-[var(--color-ink-muted)]">Permissions preview</p>
            {effective.size === 0 ? (
              <p className="mt-1 text-[13px] text-[var(--color-ink-faint)]">No permissions granted</p>
            ) : (
              <ul className="mt-1.5 space-y-1 text-[13px]">
                {catalog.permissions
                  .filter((definition) => effective.has(definition.key))
                  .map((definition) => (
                    <li key={definition.key}>{definition.label}</li>
                  ))}
              </ul>
            )}
            {role?.all_permissions && !creating ? (
              <p className="mt-2 text-[12px] text-[var(--color-ink-muted)]">And every permission added in later releases.</p>
            ) : null}
          </div>
          {role && !creating ? (
            <div>
              <p className="text-[12px] font-medium text-[var(--color-ink-muted)]">Held by</p>
              {role.groups.length === 0 ? (
                <p className="mt-1 text-[13px] text-[var(--color-ink-faint)]">No groups</p>
              ) : (
                <ul className="mt-1.5 space-y-1 text-[13px]">
                  {role.groups.map((group) => (
                    <li key={group.group_id}>
                      <Link to={`/settings/groups/${group.group_id}`} className="hover:underline">
                        {group.name}
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          ) : null}
        </aside>
      </div>

      {role && !creating ? (
        <ConfirmDialog
          open={confirmDelete}
          title={`Delete ${role.name}?`}
          description="Every group holding it loses it, and their members lose what it granted at their next request."
          confirmLabel="Delete role"
          danger
          pending={remove.isPending}
          onConfirm={() => remove.mutate(undefined)}
          onClose={() => setConfirmDelete(false)}
        />
      ) : null}
    </div>
  );
}
