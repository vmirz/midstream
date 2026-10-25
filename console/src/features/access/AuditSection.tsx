import { useInfiniteQuery } from '@tanstack/react-query';
import { ChevronRight, ScrollText, Search } from 'lucide-react';
import { useDeferredValue, useState, type ReactNode } from 'react';

import { describeAgent, describeDetail } from './logic';
import { AUDIT_ACTION_LABELS, tableHeaderClass } from './shared';
import { RelativeTime } from '@/components/Inline';
import { Badge, Button, EmptyState, Input, Panel, Select, Skeleton } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { cn } from '@/lib/cn';
import type { CpAuditEntry } from '@shared/types';

const ACTIONS: { value: string; label: string }[] = [
  { value: '', label: 'All activity' },
  { value: 'session.', label: 'Sign-ins' },
  { value: 'user.', label: 'Users' },
  { value: 'group.', label: 'Groups' },
  { value: 'role.', label: 'Roles' },
];

const GRID = 'grid grid-cols-[120px_minmax(0,1.2fr)_minmax(0,1.2fr)_minmax(0,1.3fr)_20px] items-center gap-4';

/** Who signed in, who failed to, and every change to people, groups and roles — newest first. */
export function AuditSection(): ReactNode {
  const [action, setAction] = useState('');
  const [outcome, setOutcome] = useState<'' | 'success' | 'failure'>('');
  const [search, setSearch] = useState('');
  const query = useDeferredValue(search.trim());

  const pages = useInfiniteQuery({
    queryKey: ['audit', action, outcome, query],
    queryFn: ({ pageParam }) =>
      api.audit({
        limit: 50,
        before: pageParam ?? undefined,
        action: action || undefined,
        outcome: outcome || undefined,
        q: query || undefined,
      }),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next,
  });
  const entries = pages.data?.pages.flatMap((page) => page.entries) ?? [];
  const retention = pages.data?.pages[0]?.retention_days;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <Select aria-label="Activity" value={action} onChange={(event) => setAction(event.target.value)} className="w-auto">
          {ACTIONS.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </Select>
        <Select aria-label="Outcome" value={outcome} onChange={(event) => setOutcome(event.target.value as typeof outcome)} className="w-auto">
          <option value="">Succeeded or failed</option>
          <option value="success">Succeeded</option>
          <option value="failure">Failed</option>
        </Select>
        <div className="relative min-w-[200px] flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
          <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search by email, name or address" className="pl-9" />
        </div>
      </div>
      {retention ? <p className="text-[12px] text-[var(--color-ink-muted)]">Entries are kept for {retention} days.</p> : null}

      <Panel className="overflow-hidden">
        <div className={cn(GRID, tableHeaderClass)}>
          <span>When</span>
          <span>Event</span>
          <span>Who</span>
          <span>What</span>
          <span />
        </div>
        {pages.isLoading ? (
          <div className="space-y-px p-5">
            {Array.from({ length: 6 }, (_, index) => (
              <Skeleton key={index} className="h-10" />
            ))}
          </div>
        ) : pages.error ? (
          <EmptyState title="The audit log could not be loaded" description={(pages.error as Error).message} />
        ) : entries.length === 0 ? (
          <EmptyState icon={<ScrollText className="size-6" />} title="Nothing recorded" description="Sign-ins and changes to users, groups and roles appear here." />
        ) : (
          <ul className="divide-y divide-[var(--color-line)]">
            {entries.map((entry) => (
              <AuditRow key={entry.entry_id} entry={entry} />
            ))}
          </ul>
        )}
      </Panel>
      {pages.hasNextPage ? (
        <div className="flex justify-center">
          <Button loading={pages.isFetchingNextPage} onClick={() => void pages.fetchNextPage()}>
            Load older entries
          </Button>
        </div>
      ) : null}
    </div>
  );
}

function AuditRow({ entry }: { entry: CpAuditEntry }): ReactNode {
  const [open, setOpen] = useState(false);
  const details = Object.entries(entry.details);
  return (
    <li>
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        aria-expanded={open}
        className={cn(GRID, 'w-full px-5 py-2.5 text-left text-[13px] transition-colors hover:bg-[var(--color-raised)]')}
      >
        <RelativeTime value={entry.occurred_at} className="text-[12px] text-[var(--color-ink-muted)]" />
        <span className="flex min-w-0 items-center gap-1.5">
          <span className="truncate">{AUDIT_ACTION_LABELS[entry.action] ?? entry.action}</span>
          {entry.outcome === 'failure' ? <Badge tone="failed">Failed</Badge> : null}
        </span>
        <span className="truncate text-[var(--color-ink-muted)]">{entry.actor_email ?? entry.actor_id ?? '—'}</span>
        <span className="truncate text-[var(--color-ink-muted)]">{entry.target_label ?? '—'}</span>
        <ChevronRight className={cn('size-4 text-[var(--color-ink-faint)] transition-transform', open && 'rotate-90')} />
      </button>
      {open ? (
        <dl className="grid gap-x-6 gap-y-2 bg-[var(--color-raised)] px-5 py-3 text-[12px] sm:grid-cols-[140px_minmax(0,1fr)]">
          <dt className="text-[var(--color-ink-faint)]">When</dt>
          <dd>{new Date(entry.occurred_at).toLocaleString()}</dd>
          <dt className="text-[var(--color-ink-faint)]">From</dt>
          <dd>
            {entry.ip ?? 'Unknown address'}
            {entry.user_agent ? ` · ${describeAgent(entry.user_agent)}` : ''}
          </dd>
          {details.map(([key, value]) => (
            <DetailRow key={key} label={key} value={value} />
          ))}
          <dt className="text-[var(--color-ink-faint)]">Event</dt>
          <dd className="numeric">{entry.action}</dd>
        </dl>
      ) : null}
    </li>
  );
}

function DetailRow({ label, value }: { label: string; value: unknown }): ReactNode {
  return (
    <>
      <dt className="text-[var(--color-ink-faint)]">{label.replace(/_/g, ' ')}</dt>
      <dd className="min-w-0 break-words">{describeDetail(value)}</dd>
    </>
  );
}
