import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, Lock } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { Badge } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { ApiError } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { useApi } from '@/lib/domains';
import type { CpUser, IdentityChangeResult } from '@shared/types';

export { AUDIT_ACTION_LABELS, auditActionLabel, describeAgent, expandPermissions, generatePassword, permissionMap } from './logic';

/** Every identity query, so a change can refresh what it may have changed. */
export const IDENTITY_QUERY_ROOTS = ['users', 'user', 'groups', 'group', 'roles', 'role', 'audit'] as const;

/** The permission catalog, which only changes with a control-plane release. */
export function useCatalog() {
  const api = useApi();
  return useQuery({
    queryKey: ['permissions'],
    queryFn: () => api.permissions(),
    staleTime: Number.POSITIVE_INFINITY,
  });
}

/**
 * A change to users, groups or roles: send it, say how it went, and refresh whatever it touched —
 * including who you are, since a change to your own groups changes what you may do.
 */
export function useIdentityChange<TInput>(
  send: (input: TInput) => Promise<IdentityChangeResult>,
  options: { success: (input: TInput, result: IdentityChangeResult) => string; failure: string; onApplied?: (result: IdentityChangeResult, input: TInput) => void },
) {
  const queryClient = useQueryClient();
  const toast = useToast();
  const auth = useAuth();
  return useMutation({
    mutationFn: send,
    onSuccess: async (result, input) => {
      await Promise.all(IDENTITY_QUERY_ROOTS.map((root) => queryClient.invalidateQueries({ queryKey: [root] })));
      await auth.reload();
      if (result.status === 'pending') {
        toast.success(options.success(input, result), 'The control plane accepted the change and is still applying it.');
      } else {
        toast.success(options.success(input, result));
      }
      options.onApplied?.(result, input);
    },
    onError: (error) => {
      toast.error(options.failure, error instanceof Error ? error.message : String(error));
    },
  });
}

/** The field a refused change points at, if it names one. */
export function errorField(error: unknown): string | null {
  return error instanceof ApiError && typeof error.details.field === 'string' ? error.details.field : null;
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function UserStatusBadges({ user }: { user: Pick<CpUser, 'status' | 'locked_until' | 'must_change_password' | 'self' | 'administrator'> }): ReactNode {
  return (
    <>
      {user.status === 'deactivated' ? <Badge tone="neutral">Deactivated</Badge> : null}
      {user.locked_until ? (
        <Badge tone="failed" title={`Locked out until ${new Date(user.locked_until).toLocaleString()}`}>
          Locked out
        </Badge>
      ) : null}
      {user.must_change_password && user.status === 'active' ? (
        <Badge tone="paused" title="Signs in with a password someone else chose, and must replace it">
          Password to change
        </Badge>
      ) : null}
      {user.administrator ? <Badge tone="accent">Administrator</Badge> : null}
      {user.self ? <Badge tone="neutral">You</Badge> : null}
    </>
  );
}

/** Small labels for groups or roles, cut to `max` with a count of the rest. */
export function Chips({ items, max = 3, className }: { items: string[]; max?: number; className?: string }): ReactNode {
  if (items.length === 0) return <span className="text-[12px] text-[var(--color-ink-faint)]">None</span>;
  const shown = items.slice(0, max);
  const rest = items.length - shown.length;
  return (
    <span className={cn('flex min-w-0 flex-wrap items-center gap-1', className)}>
      {shown.map((item) => (
        <span
          key={item}
          title={item}
          className="max-w-[180px] truncate rounded-md bg-[var(--color-raised)] px-1.5 py-0.5 text-[12px] text-[var(--color-ink-muted)] ring-1 ring-[var(--color-line)] ring-inset"
        >
          {item}
        </span>
      ))}
      {rest > 0 ? (
        <span title={items.slice(max).join(', ')} className="text-[12px] text-[var(--color-ink-faint)]">
          +{rest}
        </span>
      ) : null}
    </span>
  );
}

export function BuiltInMark({ label = 'Built in: cannot be renamed or deleted' }: { label?: string }): ReactNode {
  return <Lock className="size-3 shrink-0 text-[var(--color-ink-faint)]" aria-label={label} />;
}

/** The way back from a detail page to its list. */
export function BackLink({ to, children }: { to: string; children: ReactNode }): ReactNode {
  return (
    <Link
      to={to}
      className="inline-flex items-center gap-1 text-[12px] text-[var(--color-ink-muted)] transition-colors hover:text-[var(--color-ink)]"
    >
      <ArrowLeft className="size-3.5" />
      {children}
    </Link>
  );
}

/** A grid row for the access tables, the header and every row sharing one template. */
export const tableHeaderClass =
  'border-b border-[var(--color-line)] bg-[var(--color-raised)] px-5 py-2.5 text-[11px] font-medium tracking-wide text-[var(--color-ink-muted)] uppercase';

export function formatDate(value: string | null | undefined): string {
  if (!value || value.startsWith('1970-')) return '—';
  return new Date(value).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}

