import { AlertTriangle, ArrowRight, Home } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { DomainDot } from '@/components/DomainChip';
import { Badge, EmptyState, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { useConsoleAuth } from '@/lib/auth';
import { domainStatusText, strayKeys } from '@/lib/domainStatus';
import { domainPath, useConsoleConfig } from '@/lib/domains';

/**
 * Every domain this console serves (handbook 20): whether its control plane can be reached, the
 * release it runs, the key it signs access tokens with, and whether you are signed in there.
 */
export function DomainsSection(): ReactNode {
  const config = useConsoleConfig();
  const auth = useConsoleAuth();
  const domains = config.data?.domains ?? [];
  const stray = strayKeys(domains);

  if (config.isLoading) {
    return (
      <Panel className="space-y-3 p-5">
        <Skeleton className="h-4 w-1/3" />
        <Skeleton className="h-4 w-2/3" />
      </Panel>
    );
  }
  if (!config.data) {
    return (
      <Panel>
        <EmptyState title="The domains could not be read" description={config.error instanceof Error ? config.error.message : undefined} />
      </Panel>
    );
  }

  return (
    <div className="space-y-5">
      {config.data.federationKeysAgree === false ? (
        <div
          role="alert"
          className="flex items-start gap-3 rounded-[var(--radius-panel)] border px-4 py-3 text-[13px] leading-relaxed"
          style={{
            borderColor: 'color-mix(in srgb, var(--color-phase-failed) 35%, transparent)',
            background: 'color-mix(in srgb, var(--color-phase-failed) 8%, transparent)',
          }}
        >
          <AlertTriangle className="mt-0.5 size-4 shrink-0 text-[var(--color-phase-failed)]" />
          <p>
            <strong className="font-medium">The domains sign access tokens with different keys.</strong> A token made in one
            domain is refused in another. Give every domain&rsquo;s auth Secret the same <code>federation-key</code> and restart
            their control planes.
          </p>
        </div>
      ) : null}
      <Panel>
        <PanelHeader
          title={`${domains.length} ${domains.length === 1 ? 'domain' : 'domains'}`}
          description="The console's registry, from its Helm values. Each domain is a Midstream deployment with its own control plane, lake and accounts."
        />
        <ul className="divide-y divide-[var(--color-line)]">
          {domains.map((domain) => {
            const you = auth.forDomain(domain.id);
            return (
              <li key={domain.id} className="grid gap-3 px-5 py-4 md:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)_minmax(0,1fr)] md:items-center md:gap-6">
                <div className="min-w-0">
                  <p className="flex items-center gap-2 text-[13px] font-medium">
                    <DomainDot domain={domain} />
                    <span className="truncate">{domain.name}</span>
                    {domain.settingsHome ? (
                      <Badge title="Stores this console's own settings">
                        <Home className="mr-1 inline size-3" />
                        Settings home
                      </Badge>
                    ) : null}
                  </p>
                  <p className="mt-0.5 text-[12px] text-[var(--color-ink-muted)]">
                    <code className="numeric">{domain.id}</code> · {domainStatusText(domain)}
                  </p>
                </div>
                <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-0.5 text-[12px]">
                  <dt className="text-[var(--color-ink-faint)]">Version</dt>
                  <dd className="numeric truncate">{domain.version ?? '—'}</dd>
                  <dt className="text-[var(--color-ink-faint)]">Signing key</dt>
                  <dd className={stray.has(domain.federationKeyId ?? '') ? 'numeric text-[var(--color-phase-failed)]' : 'numeric'}>
                    {domain.federationKeyId ?? '—'}
                    {stray.has(domain.federationKeyId ?? '') ? ' · differs' : ''}
                  </dd>
                </dl>
                <div className="flex flex-wrap items-center justify-between gap-2 text-[12px] md:justify-end">
                  {you.signedIn ? (
                    <span className="truncate text-[var(--color-ink-muted)]">Signed in as {you.principal.email}</span>
                  ) : domain.status === 'misconfigured' ? null : (
                    <Link to={`/sign-in/${encodeURIComponent(domain.id)}`} className="font-medium text-[var(--color-accent)] hover:underline">
                      Sign in
                    </Link>
                  )}
                  {domain.status === 'misconfigured' ? null : (
                    <Link
                      to={domainPath(domain.id)}
                      className="inline-flex items-center gap-1 font-medium text-[var(--color-ink)] hover:underline"
                      aria-label={`Open ${domain.name}`}
                    >
                      Open
                      <ArrowRight className="size-3.5" />
                    </Link>
                  )}
                </div>
              </li>
            );
          })}
        </ul>
      </Panel>
    </div>
  );
}
