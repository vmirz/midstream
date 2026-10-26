import { AlertTriangle, ArrowRight } from 'lucide-react';
import { useState, type FormEvent, type ReactNode } from 'react';
import { Link, Navigate, useLocation, useNavigate, useParams } from 'react-router-dom';

import { PasswordInput } from './PasswordInput';
import { SsoOutcomePanel, useSsoOutcome } from './SsoOutcome';
import { Button, Field, Input, Select, Spinner } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useConsoleAuth } from '@/lib/auth';
import { useConsoleConfig } from '@/lib/domains';

/** Where to go after signing in: the page that asked for it, else the start page. */
function destination(state: unknown): string {
  const from = (state as { from?: { pathname?: string; search?: string } } | null)?.from;
  if (!from?.pathname || from.pathname.startsWith('/sign-in')) return '/';
  return `${from.pathname}${from.search ?? ''}`;
}

/** The domain a page under `/d/<domain>/` is in, for a sign-in it sent here. */
function domainOfPath(pathname: string | undefined): string | null {
  const match = /^\/d\/([^/]+)/.exec(pathname ?? '');
  return match ? decodeURIComponent(match[1] as string) : null;
}

/**
 * Sign in (handbook 20): through a single sign-on provider, to every domain that admits you at once,
 * or with an email and password, to one domain at a time - the one named in the path, the one the
 * page that sent you here is in, or the one chosen.
 *
 * Accounts are made by each domain's administrators, or at a provider sign-in where a domain's
 * policy allows. Someone who only needs to look can continue as a guest, when guests may see
 * anything at all.
 */
export function SignInPage(): ReactNode {
  const { domain: pathDomain } = useParams<{ domain?: string }>();
  const consoleAuth = useConsoleAuth();
  const config = useConsoleConfig();
  const location = useLocation();
  const navigate = useNavigate();
  const domains = (config.data?.domains ?? []).filter((domain) => domain.status !== 'misconfigured');
  const fromDomain = domainOfPath((location.state as { from?: { pathname?: string } } | null)?.from?.pathname);
  const preferred =
    domains.find((domain) => domain.id === pathDomain) ??
    domains.find((domain) => domain.id === fromDomain) ??
    domains.find((domain) => !consoleAuth.forDomain(domain.id).signedIn) ??
    domains[0] ??
    null;
  const [chosen, setChosen] = useState<string | null>(null);
  const domain = domains.find((entry) => entry.id === chosen) ?? preferred;
  const auth = consoleAuth.forDomain(domain?.id ?? '');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const { outcome } = useSsoOutcome();

  if (config.isLoading || consoleAuth.loading) {
    return (
      <div className="flex min-h-full items-center justify-center">
        <Spinner className="size-5 text-[var(--color-ink-faint)]" />
      </div>
    );
  }
  if (pathDomain && !domains.some((entry) => entry.id === pathDomain)) return <Navigate to="/sign-in" replace state={location.state} />;
  // Signed in where the path asks already: nothing to do here.
  if (pathDomain && domain && auth.signedIn && !auth.principal.password_change_required) {
    return <Navigate to={destination(location.state)} replace />;
  }

  const many = domains.length > 1;
  // A domain the browser has no session in answers as its guest; any guest that may see something
  // lets the person look around unsigned.
  const guestCanLook = Object.values(consoleAuth.session?.domains ?? {}).some(
    (entry) => entry.state && !entry.state.authenticated && (entry.state.principal.permissions.length > 0 || entry.state.principal.all_permissions),
  );
  const providers = consoleAuth.providers;

  const submit = async (event: FormEvent): Promise<void> => {
    event.preventDefault();
    if (!domain) return;
    setError(null);
    setSubmitting(true);
    try {
      await consoleAuth.forDomain(domain.id).signIn(email.trim(), password);
      navigate(destination(location.state), { replace: true });
    } catch (cause) {
      setError(signInMessage(cause, many ? domain.name : null));
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
          <h1 className="mt-4 text-[20px] font-semibold tracking-tight">
            {pathDomain && many && domain ? `Sign in to ${domain.name}` : 'Sign in to Midstream'}
          </h1>
          <p className="mt-1 text-[13px] text-[var(--color-ink-muted)]">
            {providers.length > 0
              ? many
                ? 'Your company account signs you in to every domain that admits you.'
                : 'Use your company account, or the account an administrator made for you.'
              : 'Use the account an administrator made for you.'}
          </p>
        </div>

        <div className="panel space-y-4 p-6 shadow-sm">
          {outcome && !error ? <SsoOutcomePanel outcome={outcome} /> : null}
          {providers.length > 0 ? (
            <div className="space-y-2">
              {providers.map((provider) => (
                <Button
                  key={provider.id}
                  type="button"
                  className="w-full"
                  onClick={() => {
                    // The provider's pages are a full navigation away, and the way back is the callback.
                    window.location.assign(`/api/auth/sso/${provider.id}/start?return=${encodeURIComponent(destination(location.state))}`);
                  }}
                >
                  Sign in with {provider.label}
                </Button>
              ))}
              <p className="flex items-center gap-3 pt-2 text-[11px] tracking-wide text-[var(--color-ink-faint)] uppercase">
                <span className="h-px flex-1 bg-[var(--color-line)]" />
                or with a password
                <span className="h-px flex-1 bg-[var(--color-line)]" />
              </p>
            </div>
          ) : null}

          <form onSubmit={(event) => void submit(event)} className="space-y-4" noValidate>
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
            {many && !pathDomain ? (
              <Field label="Domain" hint="A password signs you in to one domain; each has its own accounts.">
                <Select
                  value={domain?.id ?? ''}
                  onChange={(event) => {
                    setChosen(event.target.value);
                    setError(null);
                  }}
                >
                  {domains.map((entry) => (
                    <option key={entry.id} value={entry.id}>
                      {entry.name}
                      {consoleAuth.forDomain(entry.id).signedIn ? ' (signed in)' : ''}
                    </option>
                  ))}
                </Select>
              </Field>
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
            <Button type="submit" variant="primary" className="w-full" loading={submitting} disabled={!domain || !email.trim() || !password}>
              {many && domain ? `Sign in to ${domain.name}` : 'Sign in'}
            </Button>
          </form>
        </div>

        <div className="mt-5 space-y-2 text-center text-[12px] text-[var(--color-ink-muted)]">
          <p>Forgot your password? Ask {many && domain ? `an administrator of ${domain.name}` : 'an administrator'} to set a new one.</p>
          {pathDomain && many ? (
            <p>
              <Link to="/sign-in" state={location.state} className="font-medium text-[var(--color-ink)] hover:underline">
                Sign in to another domain
              </Link>
            </p>
          ) : null}
          {consoleAuth.overall.signedIn ? (
            <Link to={destination(location.state)} className="inline-flex items-center gap-1 font-medium text-[var(--color-ink)] hover:underline">
              Back to the console
              <ArrowRight className="size-3.5" />
            </Link>
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

function signInMessage(cause: unknown, domainName: string | null): string {
  if (!(cause instanceof ApiError)) return 'Signing in failed. Check your connection and try again.';
  if (cause.status === 503) {
    return `Signing in is not available right now: ${domainName ? `${domainName}'s` : 'the'} control plane cannot be reached.`;
  }
  return cause.message;
}
