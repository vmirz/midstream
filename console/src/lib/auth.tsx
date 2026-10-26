import { useQuery } from '@tanstack/react-query';
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { api, domainApi, onSessionLost } from './api';
import { useOptionalDomain } from './domains';
import { allQueryClients, rootQueryClient } from './queryClient';
import { can as holds, canAnywhere as holdsAnywhere } from '@shared/access';
import type { AuthState, ConsoleSession, CpPrincipal, SsoProvider } from '@shared/types';

const SESSION_KEY = ['session'] as const;

/** Who nobody is: what the page assumes until the console has said. */
const NOBODY: CpPrincipal = {
  kind: 'guest',
  user_id: null,
  email: null,
  first_name: null,
  last_name: null,
  display_name: 'Guest',
  group_ids: [],
  role_ids: [],
  permissions: [],
  all_permissions: false,
  dataset_permissions: {},
  dataset_limit: null,
  password_change_required: false,
};

/**
 * Who the person using the page is in one domain - or, on the console's own pages, across domains
 * (handbook 20). Each domain has its own session; a domain the browser has none in sees a guest.
 */
export interface Auth {
  /** The domain this is about; null for the console's own pages, which answer across domains. */
  domain: string | null;
  /** Null until the console has answered once, or while the domain cannot be asked. */
  state: AuthState | null;
  loading: boolean;
  error: Error | null;
  principal: CpPrincipal;
  signedIn: boolean;
  /** Changes whenever who the page is used by changes in any domain, so live streams reconnect as them. */
  generation: number;
  /**
   * Whether the person holds a permission: on the dataset with id `datasetId` when one is given and
   * the permission is held on datasets (`shared/access.ts`), across the domain otherwise. Across
   * domains, held in any. Presentation only: the console's server and the domains decide.
   */
  can: (permission: string, datasetId?: string | null) => boolean;
  /** Whether the person holds a permission across the domain, or on at least one of its datasets. */
  canAnywhere: (permission: string) => boolean;
  /** Sign in to this domain with a password. */
  signIn: (email: string, password: string) => Promise<AuthState>;
  /** Sign out of this domain; across domains, out of every one. */
  signOut: () => Promise<void>;
  /** Read who the browser is again, as after a change that may alter what anyone may do. */
  reload: () => Promise<void>;
  /** Take a session this domain just answered with, as after a password change. */
  adopt: (state: AuthState) => void;
}

/** Who the browser is in every domain, and the sign-in providers the console offers. */
export interface ConsoleAuth {
  session: ConsoleSession | null;
  loading: boolean;
  error: Error | null;
  generation: number;
  providers: SsoProvider[];
  /** The domains the browser has a session in, in the registry's order. */
  signedInDomains: string[];
  forDomain: (domain: string) => Auth;
  /** Across domains: what the console's own pages, which show every domain, ask. */
  overall: Auth;
  signOutEverywhere: () => Promise<void>;
  reload: () => Promise<void>;
}

const ConsoleAuthContext = createContext<ConsoleAuth | null>(null);

/**
 * The person using the page, in every domain.
 *
 * Read from `/api/auth/session`, again every minute and when the window regains focus, and at once
 * when any request finds a session gone. A change of person in any domain drops every cached answer,
 * since what the previous one could see is not what this one can.
 */
export function AuthProvider({ children }: { children: ReactNode }): ReactNode {
  const session = useQuery(
    {
      queryKey: SESSION_KEY,
      queryFn: () => api.session(),
      staleTime: 15_000,
      refetchInterval: 60_000,
      refetchOnWindowFocus: true,
      retry: 1,
    },
    rootQueryClient,
  );

  const data = session.data ?? null;
  const identity = data
    ? Object.entries(data.domains)
        .map(([id, entry]) => {
          const principal = entry.state?.principal;
          return `${id}=${principal?.kind ?? '?'}:${principal?.user_id ?? ''}:${principal?.password_change_required ?? false}`;
        })
        .join('|')
    : null;
  const [generation, setGeneration] = useState(0);
  const lastIdentity = useRef<string | null>(null);

  useEffect(() => {
    if (identity === null) return;
    if (lastIdentity.current !== null && lastIdentity.current !== identity) {
      setGeneration((value) => value + 1);
      // Everything cached was read as someone else.
      for (const client of allQueryClients()) {
        void client.invalidateQueries({ predicate: (query) => query.queryKey[0] !== SESSION_KEY[0] });
      }
    }
    lastIdentity.current = identity;
  }, [identity]);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    const unsubscribe = onSessionLost(() => {
      if (timer) return;
      timer = setTimeout(() => {
        timer = null;
        void rootQueryClient.invalidateQueries({ queryKey: SESSION_KEY });
      }, 250);
    });
    return () => {
      unsubscribe();
      if (timer) clearTimeout(timer);
    };
  }, []);

  const reload = useCallback(() => rootQueryClient.invalidateQueries({ queryKey: SESSION_KEY }), []);

  const value = useMemo<ConsoleAuth>(() => {
    const loading = session.isLoading;
    const error = (session.error as Error | null) ?? null;
    const signedInDomains = data ? Object.entries(data.domains).filter(([, entry]) => entry.state?.authenticated).map(([id]) => id) : [];

    const forDomain = (domain: string): Auth => {
      const entry = data?.domains[domain];
      const state = entry?.state ?? null;
      const principal = state?.principal ?? NOBODY;
      const adopt = (next: AuthState): void => {
        rootQueryClient.setQueryData<ConsoleSession>(SESSION_KEY, (current) =>
          current ? { ...current, domains: { ...current.domains, [domain]: { state: next, error: null } } } : current,
        );
      };
      return {
        domain,
        state,
        loading,
        error: entry?.error ? new Error(entry.error) : error,
        principal,
        signedIn: principal.kind === 'user',
        generation,
        can: (permission, datasetId) => holds(principal, permission, datasetId),
        canAnywhere: (permission) => holdsAnywhere(principal, permission),
        signIn: async (email, password) => {
          const next = await domainApi(domain).signIn(email, password);
          adopt(next);
          await reload();
          return next;
        },
        signOut: async () => {
          try {
            await domainApi(domain).signOut('current');
          } finally {
            await reload();
          }
        },
        reload,
        adopt,
      };
    };

    const states = data ? Object.values(data.domains).flatMap((entry) => (entry.state ? [entry.state] : [])) : [];
    const signedIn = states.find((state) => state.authenticated) ?? null;
    const principal = signedIn?.principal ?? states[0]?.principal ?? NOBODY;
    const overall: Auth = {
      domain: null,
      state: signedIn ?? states[0] ?? null,
      loading,
      error,
      principal,
      signedIn: signedInDomains.length > 0,
      generation,
      can: (permission, datasetId) => states.some((state) => holds(state.principal, permission, datasetId)),
      canAnywhere: (permission) => states.some((state) => holdsAnywhere(state.principal, permission)),
      signIn: async () => {
        throw new Error('sign in to a domain: each has its own session');
      },
      signOut: async () => {
        try {
          await api.signOutEverywhere();
        } finally {
          for (const client of allQueryClients()) client.removeQueries({ predicate: (query) => query.queryKey[0] !== SESSION_KEY[0] });
          await reload();
        }
      },
      reload,
      adopt: () => undefined,
    };

    return {
      session: data,
      loading,
      error,
      generation,
      providers: data?.providers ?? [],
      signedInDomains,
      forDomain,
      overall,
      signOutEverywhere: overall.signOut,
      reload,
    };
  }, [data, generation, reload, session.error, session.isLoading]);

  return <ConsoleAuthContext.Provider value={value}>{children}</ConsoleAuthContext.Provider>;
}

/** Who the browser is in every domain. */
export function useConsoleAuth(): ConsoleAuth {
  const context = useContext(ConsoleAuthContext);
  if (!context) throw new Error('useConsoleAuth must be used inside AuthProvider');
  return context;
}

/**
 * Who the person using the page is: in the page's domain, under `/d/<domain>/`; across domains on
 * the console's own pages.
 */
export function useAuth(): Auth {
  const all = useConsoleAuth();
  const domain = useOptionalDomain();
  return domain ? all.forDomain(domain.id) : all.overall;
}

/**
 * Whether the person using the page holds a permission: on a dataset, by its id, for a permission
 * held on datasets. Hides or locks parts of a page; never what keeps anything safe.
 */
export function useCan(permission: string, datasetId?: string | null): boolean {
  return useAuth().can(permission, datasetId);
}

/** Whether the person holds a permission across the domain, or on at least one of its datasets. */
export function useCanAnywhere(permission: string): boolean {
  return useAuth().canAnywhere(permission);
}

/** Initials for an avatar: first and last name, else the email. */
export function initials(principal: Pick<CpPrincipal, 'first_name' | 'last_name' | 'email'>): string {
  const fromName = `${principal.first_name?.[0] ?? ''}${principal.last_name?.[0] ?? ''}`.trim();
  if (fromName) return fromName.toUpperCase();
  return (principal.email ?? '?').slice(0, 2).toUpperCase();
}
