import { Lock } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';

import type { DomainFleet } from '@shared/types';

/**
 * The domains a page of every domain leaves out, and why: the person is not signed in there, may not
 * see them there, or the console cannot ask that domain now (handbook 20).
 */
export function HiddenDomains({ hidden, what }: { hidden: readonly DomainFleet[]; what: string }): ReactNode {
  const location = useLocation();
  if (hidden.length === 0) return null;
  const signIn = hidden.filter((entry) => entry.access === 'sign_in_required');
  const forbidden = hidden.filter((entry) => entry.access === 'forbidden');
  const unavailable = hidden.filter((entry) => entry.access === 'unavailable');
  const names = (entries: readonly DomainFleet[]) => entries.map((entry) => entry.name).join(', ');
  return (
    <div className="flex items-start gap-3 rounded-[var(--radius-panel)] border border-[var(--color-line)] bg-[var(--color-raised)] px-4 py-3 text-[13px] text-[var(--color-ink-muted)]">
      <Lock className="mt-0.5 size-4 shrink-0" />
      <div className="space-y-1">
        {signIn.length > 0 ? (
          <p>
            <Link
              to={signIn.length === 1 ? `/sign-in/${encodeURIComponent(signIn[0]!.domain)}` : '/sign-in'}
              state={{ from: location }}
              className="font-medium text-[var(--color-accent)] hover:underline"
            >
              Sign in
            </Link>{' '}
            to see the {what} of {names(signIn)}.
          </p>
        ) : null}
        {forbidden.length > 0 ? <p>You may not see the {what} of {names(forbidden)}; an administrator there can give you access.</p> : null}
        {unavailable.length > 0 ? <p>{names(unavailable)} cannot be asked right now; its {what} are left out.</p> : null}
      </div>
    </div>
  );
}
