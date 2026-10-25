import { useQuery } from '@tanstack/react-query';
import { History } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { RelativeTime } from '@/components/Inline';
import { Badge, Button, EmptyState, Panel, SegmentedControl, Skeleton } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import type { SettingSource, SettingValue, SettingView, SettingsScope } from '@shared/types';

import { SCOPE_LABELS } from './sections';
import { formatValue } from './values';

type ScopeFilter = 'all' | SettingsScope;

const SOURCE_NOTES: Record<SettingSource, string | null> = {
  override: null,
  deployment: 'deployment',
  default: 'default',
};

/** Recent changes across every scope, newest first, so an administrator can see who changed what. */
export function HistorySection({
  settings,
  retainedPerScope,
}: {
  settings: Map<string, SettingView>;
  retainedPerScope: number;
}): ReactNode {
  const [filter, setFilter] = useState<ScopeFilter>('all');
  const history = useQuery({
    queryKey: ['settings-history'],
    queryFn: () => api.settingsHistory(),
    refetchOnWindowFocus: false,
  });
  const entries = useMemo(
    () => (history.data?.changes ?? []).filter((entry) => filter === 'all' || entry.scope === filter),
    [history.data, filter],
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <SegmentedControl<ScopeFilter>
          value={filter}
          onChange={setFilter}
          options={[
            { value: 'all', label: 'All' },
            { value: 'control-plane', label: SCOPE_LABELS['control-plane'] },
            { value: 'runtime', label: SCOPE_LABELS.runtime },
            { value: 'console', label: 'Console' },
          ]}
        />
        <span className="text-[12px] text-[var(--color-ink-faint)]">
          The last {retainedPerScope} changes of each are kept.
        </span>
      </div>

      {history.isLoading ? (
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
              <li key={`${entry.scope}:${entry.revision}`} className="px-5 py-3.5">
                <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 text-[12px] text-[var(--color-ink-muted)]">
                  <span className="text-[13px] font-medium text-[var(--color-ink)]">{entry.updatedBy}</span>
                  <RelativeTime value={entry.updatedAt} />
                  <Badge>{SCOPE_LABELS[entry.scope]}</Badge>
                  <span className="numeric text-[var(--color-ink-faint)]">revision {entry.revision}</span>
                </div>
                <ul className="mt-2 space-y-1">
                  {entry.changes.map((change) => {
                    const setting = settings.get(`${entry.scope}:${change.key}`);
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
        <p className="text-[12px] text-[var(--color-phase-paused)]">
          Some history could not be read: {history.data.errors.map((error) => `${SCOPE_LABELS[error.scope]} (${error.message})`).join('; ')}
        </p>
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
