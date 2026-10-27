import { useQuery } from '@tanstack/react-query';
import { History } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { RelativeTime } from '@/components/Inline';
import { Badge, Button, EmptyState, Panel, SegmentedControl, Skeleton } from '@/components/ui/primitives';
import { api, domainApi } from '@/lib/api';
import { useConsoleAuth } from '@/lib/auth';
import { useConsoleConfig } from '@/lib/domains';
import { rootQueryClient } from '@/lib/queryClient';
import type { SettingSource, SettingValue, SettingView, SettingsHistory, SettingsOverview } from '@shared/types';

import { mergeHistories, placeOf, whereFrom, type MergedHistory } from './history';
import { formatValue } from './values';

const SOURCE_NOTES: Record<SettingSource, string | null> = {
  override: null,
  deployment: 'deployment',
  default: 'default',
};

function messageOf(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

/**
 * Recent changes, newest first, so an administrator can see who changed what: a domain's control
 * plane and runtimes on its own page; on the console's, the console's own settings and every domain's
 * the person may see, merged.
 */
export function HistorySection({ domain }: { domain: string | null }): ReactNode {
  const config = useConsoleConfig();
  const auth = useConsoleAuth();
  const [filter, setFilter] = useState('all');
  const home = config.data?.settingsHome ?? null;
  const domains = config.data?.domains ?? [];
  // On the console's page: the console's settings where its home lets the person see them, and each
  // domain's where that domain does.
  const readable = domain
    ? domains.filter((entry) => entry.id === domain)
    : domains.filter((entry) => entry.status !== 'misconfigured' && auth.forDomain(entry.id).can('settings.view'));
  const withConsole = domain === null && home !== null && auth.forDomain(home).can('settings.view');
  const key = readable.map((entry) => entry.id).join(',');

  const history = useQuery(
    {
      queryKey: ['settings-history', domain ?? 'console', key, withConsole],
      enabled: config.data !== undefined,
      refetchOnWindowFocus: false,
      queryFn: async (): Promise<MergedHistory> => {
        const read = async (
          source: { domain: string | null; domainName: string | null },
          client: { settingsHistory: () => Promise<SettingsHistory>; settings: () => Promise<SettingsOverview> },
        ) => {
          const [history, overview] = await Promise.allSettled([client.settingsHistory(), client.settings()]);
          return {
            ...source,
            history: history.status === 'fulfilled' ? history.value : null,
            overview: overview.status === 'fulfilled' ? overview.value : null,
            error: history.status === 'rejected' ? messageOf(history.reason) : null,
          };
        };
        const sources = await Promise.all([
          ...(withConsole ? [read({ domain: null, domainName: null }, api)] : []),
          ...readable.map((entry) => read({ domain: entry.id, domainName: entry.name }, domainApi(entry.id))),
        ]);
        return mergeHistories(sources);
      },
    },
    rootQueryClient,
  );
  const merged = domain === null;
  const places = useMemo(() => [...new Set((history.data?.entries ?? []).map((entry) => whereFrom(entry, merged)))], [history.data, merged]);
  const entries = useMemo(
    () => (history.data?.entries ?? []).filter((entry) => filter === 'all' || whereFrom(entry, merged) === filter),
    [history.data, filter, merged],
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        {places.length > 1 ? (
          <SegmentedControl<string>
            value={filter}
            onChange={setFilter}
            options={[{ value: 'all', label: 'All' }, ...places.map((place) => ({ value: place, label: place }))]}
          />
        ) : (
          <span />
        )}
        {history.data ? (
          <span className="text-[12px] text-[var(--color-ink-faint)]">The last {history.data.retainedPerScope} changes of each scope are kept.</span>
        ) : null}
      </div>

      {history.isLoading || config.isLoading ? (
        <Panel className="space-y-3 p-5">
          <Skeleton className="h-4 w-1/3" />
          <Skeleton className="h-4 w-2/3" />
          <Skeleton className="h-4 w-1/2" />
        </Panel>
      ) : history.error ? (
        <Panel>
          <EmptyState
            title="Change history could not be read"
            description={history.error instanceof Error ? history.error.message : String(history.error)}
            action={
              <Button size="sm" onClick={() => void history.refetch()}>
                Retry
              </Button>
            }
          />
        </Panel>
      ) : entries.length === 0 ? (
        <Panel>
          <EmptyState
            icon={<History className="size-6" />}
            title="No changes yet"
            description="Every setting still has the value its deployment gives it. Changes made on these pages are listed here."
          />
        </Panel>
      ) : (
        <Panel>
          <ol className="divide-y divide-[var(--color-line)]">
            {entries.map((entry) => (
              <li key={`${entry.domain ?? ''}:${entry.scope}:${entry.revision}`} className="px-5 py-3.5">
                <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 text-[12px] text-[var(--color-ink-muted)]">
                  <span className="text-[13px] font-medium text-[var(--color-ink)]">{entry.updatedBy}</span>
                  <RelativeTime value={entry.updatedAt} />
                  <Badge>{placeOf(entry, merged)}</Badge>
                  <span className="numeric text-[var(--color-ink-faint)]">revision {entry.revision}</span>
                </div>
                <ul className="mt-2 space-y-1">
                  {entry.changes.map((change) => {
                    const setting = history.data?.settings.get(`${entry.domain ?? ''}:${entry.scope}:${change.key}`);
                    const shape = setting ?? { kind: 'string' as const, unit: null };
                    return (
                      <li key={change.key} className="flex flex-wrap items-baseline gap-x-2 text-[13px]">
                        <span className="font-medium">{setting?.label ?? change.key}</span>
                        <ChangeValue shape={shape} value={change.before} source={change.beforeSource} muted />
                        <span className="text-[var(--color-ink-faint)]">→</span>
                        <ChangeValue shape={shape} value={change.after} source={change.afterSource} />
                      </li>
                    );
                  })}
                </ul>
              </li>
            ))}
          </ol>
        </Panel>
      )}
      {history.data && history.data.errors.length > 0 ? (
        <p className="text-[12px] text-[var(--color-phase-paused)]">Some history could not be read: {history.data.errors.join('; ')}</p>
      ) : null}
    </div>
  );
}

function ChangeValue({
  shape,
  value,
  source,
  muted = false,
}: {
  shape: Pick<SettingView, 'kind' | 'unit'>;
  value: SettingValue;
  source: SettingSource;
  muted?: boolean;
}): ReactNode {
  const note = SOURCE_NOTES[source];
  return (
    <span className={muted ? 'text-[var(--color-ink-muted)]' : undefined}>
      <span className="numeric">{formatValue(shape, value)}</span>
      {note ? <span className="ml-1 text-[12px] text-[var(--color-ink-faint)]">({note})</span> : null}
    </span>
  );
}
