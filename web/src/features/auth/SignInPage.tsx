import { AlertTriangle, ArrowRight } from 'lucide-react';
import { useState, type FormEvent, type ReactNode } from 'react';
import { Link, Navigate, useLocation, useNavigate } from 'react-router-dom';

import { PasswordInput } from './PasswordInput';
import { Button, Field, Input, Spinner } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useAuth } from '@/lib/auth';

/** Where to go after signing in: the page that asked for it, else the start page. */
function destination(state: unknown): string {
  const from = (state as { from?: { pathname?: string; search?: string } } | null)?.from;
  if (!from?.pathname || from.pathname === '/sign-in') return '/';
  return `${from.pathname}${from.search ?? ''}`;
}

/**
 * Sign in with an email and password.
 *
 * Accounts are made by an administrator, never here. Someone who only needs to look can continue as
 * a guest, when guests may see anything at all.
 */
export function SignInPage(): ReactNode {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  if (auth.signedIn && !auth.principal.password_change_required) {
    return <Navigate to={destination(location.state)} replace />;
  }

  const guestCanLook = auth.principal.kind === 'guest' && (auth.principal.permissions.length > 0 || auth.principal.all_permissions);
  const otherProviders = (auth.state?.providers ?? []).filter((provider) => provider.type !== 'password');

  const submit = async (event: FormEvent): Promise<void> => {
    event.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await auth.signIn(email.trim(), password);
      navigate(destination(location.state), { replace: true });
    } catch (cause) {
      setError(signInMessage(cause));
      setPassword('');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-full items-center justify-center bg-[var(--color-raised)] px-4 py-12">
      <div className="w-full max-w-[380px]">
        <div className="mb-6 flex flex-col items-center text-center">
          <span className="grid size-10 place-items-center rounded-[11px] bg-[var(--color-ink)]">
            <span className="size-3.5 rounded-[3px] bg-[var(--color-canvas)]" />
          </span>
          <h1 className="mt-4 text-[20px] font-semibold tracking-tight">Sign in to Materializer</h1>
          <p className="mt-1 text-[13px] text-[var(--color-ink-muted)]">Use the account an administrator made for you.</p>
        </div>

        <form onSubmit={(event) => void submit(event)} className="panel space-y-4 p-6 shadow-sm" noValidate>
          {error ? (
            <div
              role="alert"
              className="flex items-start gap-2 rounded-[var(--radius-control)] border px-3 py-2.5 text-[13px]"
              style={{
                borderColor: 'color-mix(in srgb, var(--color-phase-failed) 35%, transparent)',
                background: 'color-mix(in srgb, var(--color-phase-failed) 8%, transparent)',
              }}
            >
              <AlertTriangle className="mt-0.5 size-4 shrink-0 text-[var(--color-phase-failed)]" />
              <span>{error}</span>
            </div>
          ) : null}
          <Field label="Email">
            <Input
              type="email"
              autoComplete="username"
              autoFocus
              required
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              placeholder="you@example.com"
            />
          </Field>
          <Field label="Password">
            <PasswordInput
              autoComplete="current-password"
              required
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </Field>
          <Button type="submit" variant="primary" className="w-full" loading={submitting} disabled={!email.trim() || !password}>
            Sign in
          </Button>
          {otherProviders.map((provider) => (
            <Button key={provider.id} type="button" className="w-full" disabled>
              {provider.label}
            </Button>
          ))}
        </form>

        <div className="mt-5 space-y-2 text-center text-[12px] text-[var(--color-ink-muted)]">
          <p>Forgot your password? Ask an administrator to set a new one.</p>
          {auth.loading ? (
            <Spinner className="mx-auto size-3.5" />
          ) : guestCanLook ? (
            <Link to="/" className="inline-flex items-center gap-1 font-medium text-[var(--color-ink)] hover:underline">
              Continue as a guest
              <ArrowRight className="size-3.5" />
            </Link>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function signInMessage(cause: unknown): string {
  if (!(cause instanceof ApiError)) return 'Signing in failed. Check your connection and try again.';
  if (cause.status === 503) return 'Signing in is not available right now: the control plane cannot be reached.';
  return cause.message;
}
