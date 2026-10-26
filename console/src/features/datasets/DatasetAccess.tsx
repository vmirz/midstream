import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ShieldAlert, UserRound, Users } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { PageBody } from '@/components/AppShell';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, EmptyState, Panel, PanelHeader, Select, Skeleton } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { useApi } from '@/lib/domains';
import type { CpDatasetDetail, CpDatasetGrant, CpDatasetGrantList, CpDatasetRole } from '@shared/types';

import { useRefreshDatasets } from './useDatasets';

type GrantRole = CpDatasetGrantList['roles'][number];

/** The built-in dataset roles first, each a step up from the last, then any custom ones. */
const ROLE_ORDER = ['dataset-reader', 'dataset-writer', 'dataset-owner'];

function sortRoles<T extends Pick<CpDatasetRole, 'role_id' | 'name'>>(roles: readonly T[]): T[] {
  const rank = (role: T): number => {
    const index = ROLE_ORDER.indexOf(role.role_id);
    return index < 0 ? ROLE_ORDER.length : index;
  };
  return [...roles].sort((a, b) => rank(a) - rank(b) || a.name.localeCompare(b.name));
}

/**
 * Who may use a dataset, and how: each person or group granted a role on it. Its owners grant roles
 * they hold themselves on it; a grant to a group reaches everyone in it.
 *
 * Grants decide what the control plane, the Iceberg catalog and this console show and serve. They
 * do not protect the files: anyone with credentials that read the bucket reads every table, which
 * the page says plainly.
 */
export function DatasetAccess({ dataset }: { dataset: CpDatasetDetail }): ReactNode {
  const api = useApi();
  const grants = useQuery({ queryKey: ['dataset-grants', dataset.name], queryFn: () => api.datasetGrants(dataset.name), retry: 0 });
  const queryClient = useQueryClient();
  const refreshDatasets = useRefreshDatasets();
  const { reload } = useAuth();
  const toast = useToast();

  const save = useMutation({
    mutationFn: (input: { principalId: string; principalType: 'user' | 'group'; roleIds: string[] }) =>
      api.setDatasetGrant(dataset.name, input.principalId, { role_ids: input.roleIds, principal_type: input.principalType }),
    onSuccess: async (_result, input) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['dataset-grants', dataset.name] }),
        queryClient.invalidateQueries({ queryKey: ['dataset', dataset.name] }),
        refreshDatasets(),
        // What the person may do themselves may have changed with it.
        reload(),
      ]);
      toast.success('Access changed', input.roleIds.length ? 'The grant is saved.' : 'The grant is removed.');
    },
    onError: (failure: unknown) => toast.error('Could not change access', failure instanceof ApiError ? failure.message : 'Try again in a moment.'),
  });

  if (grants.isPending) {
    return (
      <PageBody>
        <Skeleton className="h-48" />
      </PageBody>
    );
  }
  if (grants.isError || !grants.data) {
    return (
      <PageBody>
        <Panel>
          <EmptyState
            title="Access could not be loaded"
            description={grants.error instanceof ApiError ? grants.error.message : 'Try again in a moment.'}
          />
        </Panel>
      </PageBody>
    );
  }
  const list = grants.data;
  const roles = sortRoles(list.roles);

  return (
    <PageBody className="space-y-4">
      <Panel className="overflow-hidden">
        <PanelHeader
          title="Who may use it"
          description={`A role on ${dataset.name} applies to its tables only. Reader sees schemas, queries, exports and reads through the catalog; Writer also adds and drops tables; Owner also manages the dataset and its access.`}
        />
        {list.grants.length === 0 ? (
          <EmptyState title="Nobody has a role on it" description="Only people whose roles hold these permissions everywhere can use it." />
        ) : (
          <ul className="divide-y divide-[var(--color-line)]">
            {list.grants.map((grant) => (
              <GrantRow
                key={`${grant.principal_type}:${grant.principal_id}`}
                grant={grant}
                roles={roles}
                pending={save.isPending && save.variables?.principalId === grant.principal_id}
                onSave={(roleIds) => save.mutate({ principalId: grant.principal_id, principalType: grant.principal_type, roleIds })}
              />
            ))}
          </ul>
        )}
        <AddGrant list={list} roles={roles} pending={save.isPending} onAdd={(input) => save.mutate(input)} />
      </Panel>

      <p className="flex items-start gap-2 rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] px-4 py-3 text-[12px] leading-relaxed text-[var(--color-ink-muted)]">
        <ShieldAlert className="mt-0.5 size-3.5 shrink-0" />
        <span>
          Access is enforced by the control plane, the Iceberg catalog and this console, not by the storage. Anyone holding
          credentials that read the bucket - a GCS HMAC key for DuckDB, Google credentials for PyIceberg - reads every
          table in it, whatever their roles here. Table names are not secret either: everyone who sees datasets sees them.
        </span>
      </p>
    </PageBody>
  );
}

function PrincipalLabel({ type, name, email }: { type: 'user' | 'group'; name: string; email?: string | null }): ReactNode {
  return (
    <span className="flex min-w-0 items-center gap-2">
      {type === 'group' ? (
        <Users className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" aria-label="group" />
      ) : (
        <UserRound className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" aria-label="person" />
      )}
      <span className="min-w-0 truncate">
        <span className="text-[13px] font-medium">{name}</span>
        {email ? <span className="ml-1.5 text-[12px] text-[var(--color-ink-muted)]">{email}</span> : null}
      </span>
    </span>
  );
}

function RoleChecks({
  roles,
  value,
  onChange,
  held,
}: {
  roles: GrantRole[];
  value: ReadonlySet<string>;
  onChange: (next: Set<string>) => void;
  /** Roles already held: one the person may not grant can still be kept, or taken away only by someone who may. */
  held?: ReadonlySet<string>;
}): ReactNode {
  return (
    <span className="flex flex-wrap items-center gap-1.5">
      {roles.map((role) => {
        const checked = value.has(role.role_id);
        const disabled = !role.grantable && !(held?.has(role.role_id) && checked);
        return (
          <label
            key={role.role_id}
            title={role.grantable ? role.permissions.join(', ') : 'You can only grant access you hold on this dataset yourself'}
            className={cn(
              'flex items-center gap-1.5 rounded-md border px-2 py-1 text-[12px]',
              checked ? 'border-[var(--color-accent)] text-[var(--color-ink)]' : 'border-[var(--color-line)] text-[var(--color-ink-muted)]',
              disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer',
            )}
          >
            <input
              type="checkbox"
              className="size-3 accent-[var(--color-accent)]"
              checked={checked}
              disabled={disabled || !role.grantable}
              onChange={(event) => {
                const next = new Set(value);
                if (event.target.checked) next.add(role.role_id);
                else next.delete(role.role_id);
                onChange(next);
              }}
            />
            {role.name}
          </label>
        );
      })}
    </span>
  );
}

function GrantRow({
  grant,
  roles,
  pending,
  onSave,
}: {
  grant: CpDatasetGrant;
  roles: GrantRole[];
  pending: boolean;
  onSave: (roleIds: string[]) => void;
}): ReactNode {
  const [editing, setEditing] = useState(false);
  const held = useMemo(() => new Set(grant.role_ids), [grant.role_ids]);
  const [chosen, setChosen] = useState<Set<string>>(new Set(grant.role_ids));
  // A role the person cannot grant cannot be taken away by them either.
  const canChange = grant.role_ids.every((roleId) => roles.find((role) => role.role_id === roleId)?.grantable ?? false);
  const changed = chosen.size !== held.size || [...chosen].some((roleId) => !held.has(roleId));

  return (
    <li className="flex flex-wrap items-center gap-3 px-5 py-2.5">
      <span className="min-w-[200px] flex-1">
        <PrincipalLabel type={grant.principal_type} name={grant.name} email={grant.email} />
      </span>
      {editing ? (
        <>
          <RoleChecks roles={roles} value={chosen} onChange={setChosen} held={held} />
          <span className="flex items-center gap-1.5">
            <Button size="sm" variant="ghost" onClick={() => setEditing(false)}>
              Cancel
            </Button>
            <Button
              size="sm"
              variant="primary"
              disabled={!changed}
              loading={pending}
              onClick={() => {
                onSave([...chosen]);
                setEditing(false);
              }}
            >
              Save
            </Button>
          </span>
        </>
      ) : (
        <>
          <span className="flex flex-wrap gap-1">
            {grant.roles.map((role) => (
              <Badge key={role.role_id} tone={role.role_id === 'dataset-owner' ? 'accent' : 'neutral'}>
                {role.name}
              </Badge>
            ))}
          </span>
          <span className="flex items-center gap-1.5">
            <Button
              size="sm"
              variant="ghost"
              disabled={!canChange}
              title={canChange ? undefined : 'It holds a role you cannot grant on this dataset'}
              onClick={() => {
                setChosen(new Set(grant.role_ids));
                setEditing(true);
              }}
            >
              Change
            </Button>
            <Button
              size="sm"
              variant="ghost"
              disabled={!canChange}
              loading={pending}
              onClick={() => onSave([])}
              title={`Take ${grant.name}’s roles on this dataset away`}
            >
              Remove
            </Button>
          </span>
        </>
      )}
    </li>
  );
}

function AddGrant({
  list,
  roles,
  pending,
  onAdd,
}: {
  list: CpDatasetGrantList;
  roles: GrantRole[];
  pending: boolean;
  onAdd: (input: { principalId: string; principalType: 'user' | 'group'; roleIds: string[] }) => void;
}): ReactNode {
  const granted = new Set(list.grants.map((grant) => `${grant.principal_type}:${grant.principal_id}`));
  const groups = list.candidates.groups.filter((group) => !granted.has(`group:${group.group_id}`));
  const users = list.candidates.users.filter((user) => !granted.has(`user:${user.user_id}`));
  const reader = roles.find((role) => role.role_id === 'dataset-reader' && role.grantable);
  const [principal, setPrincipal] = useState('');
  const [chosen, setChosen] = useState<Set<string>>(new Set(reader ? [reader.role_id] : []));
  if (groups.length === 0 && users.length === 0) return null;
  const [type, id] = principal.split(':') as ['user' | 'group' | '', string | undefined];

  return (
    <div className="flex flex-wrap items-center gap-3 border-t border-[var(--color-line)] bg-[var(--color-raised)] px-5 py-3">
      <Select value={principal} onChange={(event) => setPrincipal(event.target.value)} className="h-8 w-auto max-w-[320px] text-[13px]" aria-label="Person or group">
        <option value="">Add a person or group…</option>
        {groups.length > 0 ? (
          <optgroup label="Groups">
            {groups.map((group) => (
              <option key={group.group_id} value={`group:${group.group_id}`}>
                {group.name}
              </option>
            ))}
          </optgroup>
        ) : null}
        {users.length > 0 ? (
          <optgroup label="People">
            {users.map((user) => (
              <option key={user.user_id} value={`user:${user.user_id}`}>
                {user.name} · {user.email}
              </option>
            ))}
          </optgroup>
        ) : null}
      </Select>
      <RoleChecks roles={roles} value={chosen} onChange={setChosen} />
      <Button
        size="sm"
        variant="primary"
        disabled={!type || !id || chosen.size === 0}
        loading={pending}
        onClick={() => {
          if (!type || !id) return;
          onAdd({ principalId: id, principalType: type, roleIds: [...chosen] });
          setPrincipal('');
        }}
      >
        Grant
      </Button>
    </div>
  );
}
