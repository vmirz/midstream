import { LogIn, LogOut } from 'lucide-react';
import { useState, type ReactNode } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';

import { AccountSection } from './AccountSection';
import { FederatedTokens } from './FederatedTokens';
import { describeMethod } from './logic';
import { DomainDot } from '@/components/DomainChip';
import { Button, EmptyState, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { useConsoleAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { DomainProvider, useConsoleConfig } from '@/lib/domains';

/**
 * Your account, in every domain (handbook 20). Each domain keeps its own account and session, so
 * this lists the domains, signed in or not, and shows the one chosen: who you are there, what you
 * may do, your password, your sessions and your access tokens.
 */
export function AccountOverview({ domainId }: { domainId: string | null }): ReactNode {
  const config = useConsoleConfig();
  const auth = useConsoleAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const toast = useToast();
  const [leaving, setLeaving] = useState<string | null>(null);
  const domains = (config.data?.domains ?? []).filter((domain) => domain.status !== 'misconfigured');
  const signedIn = domains.filter((domain) => auth.forDomain(domain.id).signedIn);
  const chosen = domains.find((domain) => domain.id === domainId && auth.forDomain(domain.id).signedIn) ?? signedIn[0] ?? null;

  if (config.isLoading || auth.loading) {
    return (
      <Panel className="space-y-3 p-5">
        <Skeleton className="h-4 w-1/3" />
        <Skeleton className="h-4 w-2/3" />
      </Panel>
    );
  }
  if (domains.length === 1 && chosen) {
    return (
      <DomainProvider domain={chosen}>
        <AccountSection />
      </DomainProvider>
    );
  }

  const signOut = async (id: string, name: string): Promise<void> => {
    setLeaving(id);
    try {
      await auth.forDomain(id).signOut();
      toast.success(`Signed out of ${name}`);
      if (chosen?.id === id) navigate('/settings/account', { replace: true });
    } catch (cause) {
      toast.error('Signing out failed', cause instanceof Error ? cause.message : String(cause));
    } finally {
      setLeaving(null);
    }
  };

  return (
    <div className="space-y-5">
      <Panel>
        <PanelHeader
          title="Your domains"
          description="Each domain has its own account and session. Sign in through a provider to reach every domain that admits you at once, or with a password, one domain at a time."
          actions={
            signedIn.length > 1 ? (
              <Button size="sm" onClick={() => void auth.signOutEverywhere().then(() => navigate('/', { replace: true }))}>
                <LogOut className="size-3.5" />
                Sign out everywhere
              </Button>
            ) : null
          }
        />
        <ul className="divide-y divide-[var(--color-line)]">
          {domains.map((domain) => {
            const you = auth.forDomain(domain.id);
            const selected = chosen?.id === domain.id;
            return (
              <li key={domain.id} className={cn('flex flex-wrap items-center gap-3 px-5 py-3', selected && 'bg-[var(--color-raised)]')}>
                <DomainDot domain={domain} />
                <div className="min-w-0 flex-1">
                  <p className="truncate text-[13px] font-medium">{domain.name}</p>
                  <p className="truncate text-[12px] text-[var(--color-ink-muted)]">
                    {you.signedIn
                      ? `Signed in as ${you.principal.email ?? you.principal.display_name}${you.state?.session?.method ? ` · ${describeMethod(you.state.session.method)}` : ''}`
                      : you.error
                        ? `Cannot be asked now: ${you.error.message}`
                        : 'Not signed in: you see what its guests may'}
                  </p>
                </div>
                {you.signedIn ? (
                  <span className="flex items-center gap-2">
                    {selected ? null : (
                      <Link to={`/settings/account/${encodeURIComponent(domain.id)}`} className="text-[12px] font-medium text-[var(--color-accent)] hover:underline">
                        Show
                      </Link>
                    )}
                    <Button size="sm" variant="ghost" loading={leaving === domain.id} onClick={() => void signOut(domain.id, domain.name)}>
                      Sign out
                    </Button>
                  </span>
                ) : (
                  <Button size="sm" onClick={() => navigate(`/sign-in/${encodeURIComponent(domain.id)}`, { state: { from: location } })}>
                    <LogIn className="size-3.5" />
                    Sign in
                  </Button>
                )}
              </li>
            );
          })}
        </ul>
      </Panel>

      {signedIn.length > 0 ? <FederatedTokens /> : null}

      {chosen ? (
        <section className="space-y-3" aria-label={`Your account in ${chosen.name}`}>
          <h3 className="text-[13px] font-semibold tracking-tight">In {chosen.name}</h3>
          <DomainProvider key={chosen.id} domain={chosen}>
            <AccountSection />
          </DomainProvider>
        </section>
      ) : (
        <Panel>
          <EmptyState
            icon={<LogIn className="size-6" />}
            title="You are not signed in to any domain"
            description="Sign in to see your account there."
            action={
              <Button variant="primary" onClick={() => navigate('/sign-in', { state: { from: location } })}>
                Sign in
              </Button>
            }
          />
        </Panel>
      )}
    </div>
  );
}
