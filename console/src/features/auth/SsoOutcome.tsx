import { useQuery } from '@tanstack/react-query';
import { AlertTriangle, CheckCircle2, CloudOff, Lock } from 'lucide-react';
import { useEffect, useRef, type ReactNode } from 'react';
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom';

import { useToast } from '@/components/ui/Toast';
import { api } from '@/lib/api';
import { useConsoleAuth } from '@/lib/auth';
import { rootQueryClient } from '@/lib/queryClient';
import type { SsoDomainOutcome, SsoOutcome } from '@shared/types';

import { describeOutcome } from './ssoSummary';

/** The query parameter a sign-in through a provider comes back with: what it did, by id. */
export const OUTCOME_PARAM = 'signin';

/** What a sign-in through a provider did, when the address names one. */
export function useSsoOutcome(): { outcome: SsoOutcome | null; id: string | null } {
  const [params] = useSearchParams();
  const id = params.get(OUTCOME_PARAM);
  const query = useQuery(
    { queryKey: ['sso-outcome', id], queryFn: () => api.ssoOutcome(id!), enabled: id !== null, staleTime: Infinity, retry: 0 },
    rootQueryClient,
  );
  return { outcome: query.data ?? null, id };
}

/**
 * Back from a provider on any page but the sign-in page: say what the sign-in did - signed in where,
 * refused where and why - and take the outcome out of the address.
 */
export function SsoOutcomeToast(): ReactNode {
  const { outcome, id } = useSsoOutcome();
  const toast = useToast();
  const auth = useConsoleAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const shown = useRef<string | null>(null);

  useEffect(() => {
    if (!outcome || !id || shown.current === id || location.pathname.startsWith('/sign-in')) return;
    shown.current = id;
    void auth.reload();
    const summary = describeOutcome(outcome);
    if (summary.refused.length === 0) toast.success(summary.title, summary.detail ?? undefined);
    else toast.error(summary.title, summary.detail ?? undefined);
    const params = new URLSearchParams(location.search);
    params.delete(OUTCOME_PARAM);
    const search = params.toString();
    void navigate({ pathname: location.pathname, search: search ? `?${search}` : '' }, { replace: true });
  }, [outcome, id, toast, auth, navigate, location]);

  return null;
}

/** On the sign-in page: what the last sign-in through a provider did, domain by domain. */
export function SsoOutcomePanel({ outcome }: { outcome: SsoOutcome }): ReactNode {
  return (
    <div
      role="alert"
      className="space-y-2 rounded-[var(--radius-control)] border px-3 py-2.5 text-[13px]"
      style={{
        borderColor: 'color-mix(in srgb, var(--color-phase-failed) 35%, transparent)',
        background: 'color-mix(in srgb, var(--color-phase-failed) 8%, transparent)',
      }}
    >
      {outcome.error ? (
        <p className="flex items-start gap-2">
          <AlertTriangle className="mt-0.5 size-4 shrink-0 text-[var(--color-phase-failed)]" />
          <span>{outcome.error}</span>
        </p>
      ) : (
        <>
          <p className="font-medium">Signing in with {outcome.label} gave you no session here.</p>
          <ul className="space-y-1.5">
            {outcome.domains.map((entry) => (
              <DomainLine key={entry.domain} entry={entry} />
            ))}
          </ul>
        </>
      )}
    </div>
  );
}

function DomainLine({ entry }: { entry: SsoDomainOutcome }): ReactNode {
  const icon =
    entry.status === 'signed_in' ? (
      <CheckCircle2 className="mt-0.5 size-3.5 shrink-0 text-[var(--color-phase-running)]" />
    ) : entry.status === 'unreachable' ? (
      <CloudOff className="mt-0.5 size-3.5 shrink-0 text-[var(--color-phase-paused)]" />
    ) : (
      <Lock className="mt-0.5 size-3.5 shrink-0 text-[var(--color-ink-muted)]" />
    );
  return (
    <li className="flex items-start gap-2 text-[12px]">
      {icon}
      <span>
        <span className="font-medium">{entry.name}</span>
        {entry.status === 'signed_in' ? ': signed in' : entry.message ? `: ${entry.message}` : ''}
      </span>
    </li>
  );
}
