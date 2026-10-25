import { Monitor } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { RelativeTime } from '@/components/Inline';
import { Badge, Button } from '@/components/ui/primitives';
import type { CpSessionRecord } from '@shared/types';

import { describeAgent } from './logic';

const ENDED_REASONS: Record<string, string> = {
  signed_out: 'Signed out',
  signed_out_elsewhere: 'Signed out from another session',
  signed_out_everywhere: 'Signed out everywhere',
  signed_out_by_administrator: 'Signed out by an administrator',
  password_changed: 'Password changed',
  password_reset: 'Password reset',
  deactivated: 'Account deactivated',
};

/** Sessions, active ones first; ended ones are folded away until asked for. */
export function SessionList({
  sessions,
  onSignOut,
  pending = false,
}: {
  sessions: CpSessionRecord[];
  /** Offered for every active session but the current one. */
  onSignOut?: (sessionId: string) => void;
  pending?: boolean;
}): ReactNode {
  const [showEnded, setShowEnded] = useState(false);
  const active = sessions.filter((session) => session.active);
  const ended = sessions.filter((session) => !session.active);

  if (sessions.length === 0) {
    return <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">No sessions recorded yet.</p>;
  }

  return (
    <div>
      <ul className="divide-y divide-[var(--color-line)]">
        {active.map((session) => (
          <SessionRow key={session.session_id} session={session} onSignOut={onSignOut} pending={pending} />
        ))}
        {showEnded ? ended.map((session) => <SessionRow key={session.session_id} session={session} />) : null}
      </ul>
      {active.length === 0 && !showEnded ? (
        <p className="px-5 py-3 text-[13px] text-[var(--color-ink-muted)]">No active sessions.</p>
      ) : null}
      {ended.length > 0 ? (
        <button
          type="button"
          onClick={() => setShowEnded((value) => !value)}
          className="w-full border-t border-[var(--color-line)] px-5 py-2.5 text-left text-[12px] text-[var(--color-ink-muted)] transition-colors hover:text-[var(--color-ink)]"
        >
          {showEnded ? 'Hide ended sessions' : `Show ${ended.length} ended ${ended.length === 1 ? 'session' : 'sessions'}`}
        </button>
      ) : null}
    </div>
  );
}

function SessionRow({
  session,
  onSignOut,
  pending = false,
}: {
  session: CpSessionRecord;
  onSignOut?: (sessionId: string) => void;
  pending?: boolean;
}): ReactNode {
  return (
    <li className="flex flex-wrap items-center justify-between gap-3 px-5 py-3">
      <span className="flex min-w-0 items-center gap-3">
        <Monitor className="size-4 shrink-0 text-[var(--color-ink-faint)]" />
        <span className="min-w-0">
          <span className="flex flex-wrap items-center gap-1.5 text-[13px] font-medium">
            {describeAgent(session.user_agent)}
            {session.current ? <Badge tone="running">This browser</Badge> : null}
            {!session.active ? <Badge tone="neutral">{ENDED_REASONS[session.ended_reason ?? ''] ?? 'Expired'}</Badge> : null}
          </span>
          <span className="block text-[12px] text-[var(--color-ink-muted)]">
            {session.ip ?? 'Unknown address'} · signed in <RelativeTime value={session.created_at} /> · last active{' '}
            <RelativeTime value={session.last_seen_at} />
          </span>
        </span>
      </span>
      {onSignOut && session.active && !session.current ? (
        <Button size="sm" variant="ghost" disabled={pending} onClick={() => onSignOut(session.session_id)}>
          Sign out
        </Button>
      ) : null}
    </li>
  );
}
