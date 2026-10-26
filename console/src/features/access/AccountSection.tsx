import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { LogOut } from 'lucide-react';
import type { ReactNode } from 'react';

import { ChangePasswordForm } from '../auth/ChangePasswordForm';
import { AccessTokens } from './AccessTokens';
import { SessionList } from './SessionList';
import { permissionMap, useCatalog } from './shared';
import { Avatar } from '@/components/Avatar';
import { Button, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { useAuth } from '@/lib/auth';
import { useApi } from '@/lib/domains';

/**
 * Your own account: who you are signed in as, what you may do, your password, your sessions, and
 * the access tokens external engines sign in with.
 */
export function AccountSection(): ReactNode {
  const api = useApi();
  const auth = useAuth();
  const principal = auth.principal;
  const catalog = useCatalog();
  const byKey = permissionMap(catalog.data);
  const queryClient = useQueryClient();
  const toast = useToast();
  const sessions = useQuery({ queryKey: ['my-sessions'], queryFn: () => api.mySessions() });
  const signOut = useMutation({
    mutationFn: (input: { scope: 'others' | 'session'; sessionId?: string }) => api.signOut(input.scope, input.sessionId),
    onSuccess: (_, input) => {
      toast.success(input.scope === 'others' ? 'Your other sessions were signed out' : 'Session signed out');
      // The sign-out is recorded a moment after it is accepted.
      setTimeout(() => void queryClient.invalidateQueries({ queryKey: ['my-sessions'] }), 500);
    },
    onError: (error) => toast.error('Signing out failed', error instanceof Error ? error.message : String(error)),
  });
  const others = (sessions.data?.sessions ?? []).filter((session) => session.active && !session.current).length;

  return (
    <div className="space-y-5">
      <Panel>
        <div className="flex flex-wrap items-center gap-4 px-5 py-4">
          <Avatar name={principal.display_name} email={principal.email} size="lg" />
          <div className="min-w-0 flex-1">
            <p className="truncate text-[15px] font-semibold tracking-tight">{principal.display_name}</p>
            <p className="truncate text-[13px] text-[var(--color-ink-muted)]">{principal.email}</p>
          </div>
        </div>
        <p className="border-t border-[var(--color-line)] px-5 py-3 text-[12px] text-[var(--color-ink-muted)]">
          An administrator manages your name, email and groups. Ask one if anything here should change.
        </p>
      </Panel>

      <Panel>
        <PanelHeader
          title="What you can do"
          description={principal.all_permissions ? 'Everything: you are an administrator.' : 'Your groups give you these permissions.'}
        />
        {catalog.isLoading ? (
          <div className="p-5">
            <Skeleton className="h-16" />
          </div>
        ) : (
          <div className="divide-y divide-[var(--color-line)]">
            {(catalog.data?.categories ?? []).map((category) => {
              const held = (catalog.data?.permissions ?? []).filter(
                (permission) => permission.category === category.id && (principal.all_permissions || principal.permissions.includes(permission.key)),
              );
              if (held.length === 0) return null;
              return (
                <div key={category.id} className="px-5 py-3">
                  <p className="mb-1.5 text-[11px] font-medium tracking-wide text-[var(--color-ink-faint)] uppercase">{category.label}</p>
                  <div className="flex flex-wrap gap-1.5">
                    {held.map((permission) => (
                      <span
                        key={permission.key}
                        title={byKey.get(permission.key)?.description}
                        className="rounded-md bg-[var(--color-raised)] px-2 py-1 text-[12px] text-[var(--color-ink-muted)] ring-1 ring-[var(--color-line)] ring-inset"
                      >
                        {permission.label}
                      </span>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Panel>

      <Panel>
        <PanelHeader title="Password" description="Changing it signs you out everywhere else." />
        <div className="max-w-md px-5 py-4">
          <ChangePasswordForm />
        </div>
      </Panel>

      <Panel>
        <PanelHeader
          title="Sessions"
          description="Where you are signed in. Sign out of any you do not recognise, and change your password."
          actions={
            others > 0 ? (
              <Button size="sm" loading={signOut.isPending} onClick={() => signOut.mutate({ scope: 'others' })}>
                <LogOut className="size-3.5" />
                Sign out other sessions
              </Button>
            ) : null
          }
        />
        {sessions.isLoading ? (
          <div className="p-5">
            <Skeleton className="h-12" />
          </div>
        ) : (
          <SessionList
            sessions={sessions.data?.sessions ?? []}
            onSignOut={(sessionId) => signOut.mutate({ scope: 'session', sessionId })}
            pending={signOut.isPending}
          />
        )}
      </Panel>

      {auth.canAnywhere('catalog.read') ? <AccessTokens /> : null}

      <div className="flex justify-end">
        <Button variant="ghost" onClick={() => void auth.signOut()}>
          <LogOut className="size-4" />
          Sign out
        </Button>
      </div>
    </div>
  );
}
