import { QueryClientProvider, useQuery, type UseQueryResult } from '@tanstack/react-query';
import { CloudOff, Compass } from 'lucide-react';
import { createContext, useContext, useMemo, type ReactNode } from 'react';
import { Link, useLocation, useParams } from 'react-router-dom';

import { PageBody } from '@/components/AppShell';
import { EmptyState, Panel, Spinner } from '@/components/ui/primitives';
import { api, domainApi, type DomainApi } from './api';
import { queryClientFor, rootQueryClient } from './queryClient';
import { domainPath } from './paths';
import type { ConsoleConfig, ConsoleDomain } from '@shared/types';

export { domainPath, sectionOf, sectionPath, type ShellSection } from './paths';

/**
 * The console's config: every domain it serves, as this person may know each (handbook 20). Kept a
 * minute in the console's own cache, whatever page asks.
 */
export function useConsoleConfig(): UseQueryResult<ConsoleConfig> {
  return useQuery({ queryKey: ['config'], queryFn: () => api.config(), staleTime: 60_000, refetchInterval: 60_000, retry: 0 }, rootQueryClient);
}

/** Every domain the console serves, in the registry's order; empty until the config arrives. */
export function useDomains(): ConsoleDomain[] {
  return useConsoleConfig().data?.domains ?? [];
}

/** Whether the console serves more than one domain: names then say which domain they are in. */
export function useManyDomains(): boolean {
  return useDomains().length > 1;
}

const DomainContext = createContext<ConsoleDomain | null>(null);

/**
 * A page of one domain: everything under `/d/<domain>/`. It gives the page its domain, its API and a
 * query cache of its own, so nothing one domain answered is ever shown as another's.
 */
export function DomainScope({ children }: { children: ReactNode }): ReactNode {
  const { domain: id } = useParams();
  const config = useConsoleConfig();
  const domain = config.data?.domains.find((candidate) => candidate.id === id) ?? null;

  if (config.isLoading) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center">
        <Spinner className="size-5 text-[var(--color-ink-faint)]" />
      </div>
    );
  }
  if (!domain) {
    return (
      <PageBody>
        <Panel>
          <EmptyState
            icon={<Compass className="size-6" />}
            title={`This console serves no domain ${id ?? ''}`}
            description="The link may be old, or the domain was removed from the console's registry."
            action={
              <Link className="text-[13px] font-medium text-[var(--color-accent)] hover:underline" to="/">
                See every domain
              </Link>
            }
          />
        </Panel>
      </PageBody>
    );
  }
  if (domain.status === 'misconfigured') {
    return (
      <PageBody>
        <Panel>
          <EmptyState icon={<CloudOff className="size-6" />} title={`${domain.name} is not available`} description={domain.statusMessage ?? undefined} />
        </Panel>
      </PageBody>
    );
  }
  return <DomainProvider domain={domain}>{children}</DomainProvider>;
}

/**
 * Part of a page about one domain, outside `/d/<domain>/`: a dialog opened from an all-domains page
 * for the domain chosen, or a domain's section of the Account page. It gets what a domain's page
 * gets: the domain, its API and its query cache.
 */
export function DomainProvider({ domain, children }: { domain: ConsoleDomain; children: ReactNode }): ReactNode {
  const client = useMemo(() => queryClientFor(domain.id), [domain.id]);
  return (
    <DomainContext.Provider value={domain}>
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    </DomainContext.Provider>
  );
}

/** The domain of the page, inside `/d/<domain>/`; null on the console's own pages. */
export function useOptionalDomain(): ConsoleDomain | null {
  return useContext(DomainContext);
}

/** The domain of the page. Only pages under `/d/<domain>/` have one. */
export function useDomain(): ConsoleDomain {
  const domain = useContext(DomainContext);
  if (!domain) throw new Error('useDomain must be used inside a DomainScope');
  return domain;
}

/** The API of the page's domain. */
export function useApi(): DomainApi {
  const { id } = useDomain();
  return useMemo(() => domainApi(id), [id]);
}

/** A link inside the page's domain: `/d/<domain>/<path>`. */
export function useDomainPath(): (path: string) => string {
  const { id } = useDomain();
  return useMemo(() => (path: string) => domainPath(id, path), [id]);
}

/**
 * The domain the address names, for what sits around a page rather than in it - the shell's switcher
 * and navigation: `/d/<domain>/…` names one; every other page is the console's own.
 */
export function useScopeDomainId(): string | null {
  const { pathname } = useLocation();
  const match = /^\/d\/([^/]+)/.exec(pathname);
  return match ? decodeURIComponent(match[1] as string) : null;
}
