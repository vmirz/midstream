import { CloudOff, Lock, LogIn } from 'lucide-react';
import type { ReactNode } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router-dom';

import { ChangePasswordForm } from './ChangePasswordForm';
import { PageBody } from '@/components/AppShell';
import { Button, EmptyState, Panel, Spinner } from '@/components/ui/primitives';
import { useAuth } from '@/lib/auth';

/**
 * Everything behind the sign-in page: waits for the console to say who is using the page, and
 * holds back everything else while a password someone else chose is still in use.
 */
export function SessionGate({ children }: { children: ReactNode }): ReactNode {
  const auth = useAuth();

  if (auth.loading) {
    return (
      <div className="flex min-h-full items-center justify-center">
        <Spinner className="size-5 text-[var(--color-ink-faint)]" />
      </div>
    );
  }
  if (!auth.state && auth.error) {
    return (
      <div className="flex min-h-full items-center justify-center px-4">
        <Panel className="w-full max-w-md">
          <EmptyState
            icon={<CloudOff className="size-6" />}
            title="The console cannot tell who you are"
            description={`It checks sign-ins with the control plane, which did not answer: ${auth.error.message}`}
            action={<Button onClick={() => void auth.reload()}>Try again</Button>}
          />
        </Panel>
      </div>
    );
  }
  if (auth.principal.password_change_required) return <PasswordChangeScreen />;
  return children;
}

function PasswordChangeScreen(): ReactNode {
  const auth = useAuth();
  return (
    <div className="flex min-h-full items-center justify-center bg-[var(--color-raised)] px-4 py-12">
      <div className="w-full max-w-[400px]">
        <div className="mb-6 text-center">
          <span className="mx-auto grid size-10 place-items-center rounded-full bg-[var(--color-accent-soft)] text-[var(--color-accent)]">
            <Lock className="size-5" />
          </span>
          <h1 className="mt-4 text-[20px] font-semibold tracking-tight">Choose a new password</h1>
          <p className="mt-1 text-[13px] leading-relaxed text-[var(--color-ink-muted)]">
            An administrator set the password you signed in with, {auth.principal.first_name ?? 'there'}. Choose your own to
            continue: only you should know it.
          </p>
        </div>
        <div className="panel p-6 shadow-sm">
          <ChangePasswordForm submitLabel="Set password and continue" />
        </div>
        <p className="mt-5 text-center text-[12px] text-[var(--color-ink-muted)]">
          Not you?{' '}
          <button type="button" className="font-medium text-[var(--color-ink)] hover:underline" onClick={() => void auth.signOut()}>
            Sign out
          </button>
        </p>
      </div>
    </div>
  );
}

/**
 * A page that needs a permission. A guest without it is sent to sign in, and brought back after;
 * a signed-in person without it is told so, and who could change that.
 */
export function RequirePermission({
  permission,
  what,
  children,
}: {
  permission: string;
  /** What the page shows, as a sentence would name it: "materializations". */
  what: string;
  children: ReactNode;
}): ReactNode {
  const auth = useAuth();
  const location = useLocation();
  if (auth.can(permission)) return children;
  if (!auth.signedIn) return <Navigate to="/sign-in" replace state={{ from: location }} />;
  return <AccessDenied what={what} />;
}

export function AccessDenied({ what }: { what: string }): ReactNode {
  return (
    <PageBody>
      <Panel>
        <EmptyState
          icon={<Lock className="size-6" />}
          title={`You do not have access to ${what}`}
          description="Your groups do not grant it. An administrator can add you to a group that does."
        />
      </Panel>
    </PageBody>
  );
}

/** Shown in place of something guests cannot see: an invitation to sign in. */
export function SignInPrompt({ what }: { what: string }): ReactNode {
  const navigate = useNavigate();
  const location = useLocation();
  return (
    <Panel>
      <EmptyState
        icon={<LogIn className="size-6" />}
        title={`Sign in to see ${what}`}
        description="Signed in, you see whatever your groups allow."
        action={
          <Button variant="primary" onClick={() => navigate('/sign-in', { state: { from: location } })}>
            Sign in
          </Button>
        }
      />
    </Panel>
  );
}
