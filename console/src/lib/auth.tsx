import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';

import { api, onSessionLost } from './api';
import { can as holds, canAnywhere as holdsAnywhere } from '@shared/access';
import type { AuthState, CpPrincipal } from '@shared/types';

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

export interface Auth {
  /** Null until the console has answered once. */
  state: AuthState | null;
  loading: boolean;
  error: Error | null;
  principal: CpPrincipal;
  signedIn: boolean;
  /** Changes whenever the person using the page changes, so live streams reconnect as them. */
  generation: number;
  /**
   * Whether the person holds a permission: on the dataset with id `datasetId` when one is given and
   * the permission is held on datasets (`shared/access.ts`), across the deployment otherwise.
   * Presentation only: the console's server and the control plane decide.
   */
  can: (permission: string, datasetId?: string | null) => boolean;
  /** Whether the person holds a permission across the deployment, or on at least one dataset. */
  canAnywhere: (permission: string) => boolean;
  signIn: (email: string, password: string) => Promise<AuthState>;
  signOut: () => Promise<void>;
  /** Read the session again, as after a change that may alter what you may do. */
  reload: () => Promise<void>;
  /** Take a session the console just answered with, as after a password change. */
  adopt: (state: AuthState) => void;
}

const AuthContext = createContext<Auth | null>(null);

/**
 * The signed-in person, or a guest, for the whole page.
 *
 * Read from `/api/auth/session`, again every minute and when the window regains focus, and at once
 * when any request finds the session gone. A change of person drops every cached answer, since
 * what the previous one could see is not what this one can.
 */
export function AuthProvider({ children }: { children: ReactNode }): ReactNode {
  const queryClient = useQueryClient();
  const session = useQuery({
    queryKey: SESSION_KEY,
    queryFn: () => api.session(),
    staleTime: 15_000,
    refetchInterval: 60_000,
    refetchOnWindowFocus: true,
    retry: 1,
  });

  const state = session.data ?? null;
  const principal = state?.principal ?? NOBODY;
  const identity = `${principal.kind}:${principal.user_id ?? ''}:${principal.password_change_required}`;
  const [generation, setGeneration] = useState(0);
  const lastIdentity = useRef<string | null>(null);

  useEffect(() => {
    if (!state) return;
    if (lastIdentity.current !== null && lastIdentity.current !== identity) {
      setGeneration((value) => value + 1);
      // Everything cached was read as someone else.
      void queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] !== SESSION_KEY[0] });
    }
    lastIdentity.current = identity;
  }, [identity, queryClient, state]);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    const unsubscribe = onSessionLost(() => {
      if (timer) return;
      timer = setTimeout(() => {
        timer = null;
        void queryClient.invalidateQueries({ queryKey: SESSION_KEY });
      }, 250);
    });
    return () => {
      unsubscribe();
      if (timer) clearTimeout(timer);
    };
  }, [queryClient]);

  const adopt = useCallback((next: AuthState) => queryClient.setQueryData(SESSION_KEY, next), [queryClient]);

  const value = useMemo<Auth>(() => {
    const can = (permission: string, datasetId?: string | null): boolean => holds(principal, permission, datasetId);
    const canAnywhere = (permission: string): boolean => holdsAnywhere(principal, permission);
    return {
      state,
      loading: session.isLoading,
      error: (session.error as Error | null) ?? null,
      principal,
      signedIn: principal.kind === 'user',
      generation,
      can,
      canAnywhere,
      signIn: async (email, password) => {
        const next = await api.signIn(email, password);
        adopt(next);
        return next;
      },
      signOut: async () => {
        try {
          await api.signOut('current');
        } finally {
          queryClient.removeQueries({ predicate: (query) => query.queryKey[0] !== SESSION_KEY[0] });
          await queryClient.invalidateQueries({ queryKey: SESSION_KEY });
        }
      },
      reload: () => queryClient.invalidateQueries({ queryKey: SESSION_KEY }),
      adopt,
    };
  }, [adopt, generation, principal, queryClient, session.error, session.isLoading, state]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): Auth {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used inside AuthProvider');
  return context;
}

/**
 * Whether the person using the page holds a permission: on a dataset, by its id, for a permission
 * held on datasets. Hides or locks parts of a page; never what keeps anything safe.
 */
export function useCan(permission: string, datasetId?: string | null): boolean {
  return useAuth().can(permission, datasetId);
}

/** Whether the person holds a permission across the deployment, or on at least one dataset. */
export function useCanAnywhere(permission: string): boolean {
  return useAuth().canAnywhere(permission);
}

/** Initials for an avatar: first and last name, else the email. */
export function initials(principal: Pick<CpPrincipal, 'first_name' | 'last_name' | 'email'>): string {
  const fromName = `${principal.first_name?.[0] ?? ''}${principal.last_name?.[0] ?? ''}`.trim();
  if (fromName) return fromName.toUpperCase();
  return (principal.email ?? '?').slice(0, 2).toUpperCase();
}
