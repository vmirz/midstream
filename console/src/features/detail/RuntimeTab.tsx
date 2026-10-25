import type { ReactNode } from 'react';

import { PageBody } from '@/components/AppShell';
import { Identifier, RelativeTime } from '@/components/Inline';
import { Badge, EmptyState, KeyValue, Panel, PanelHeader } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { humanize, topicsLabel } from '@/lib/format';
import type { CpResourceCondition, DetailSnapshot } from '@shared/types';

const AGENT_TONE: Record<string, string> = {
  running: 'var(--color-phase-running)',
  paused: 'var(--color-phase-paused)',
  stopped: 'var(--color-phase-idle)',
  failed: 'var(--color-phase-failed)',
};

export function RuntimeTab({ snapshot }: { snapshot: DetailSnapshot }): ReactNode {
  const detail = snapshot.detail;
  const observation = detail?.status.resource_observation;
  const binding = detail?.runtime_binding;

  if (!detail) {
    return (
      <PageBody>
        <EmptyState title="Not found" description="This materialization is no longer present." />
      </PageBody>
    );
  }

  const agents = Object.entries(observation?.managed_agent_states ?? {});

  return (
    <PageBody className="space-y-6">
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <Panel>
          <PanelHeader
            title="Kubernetes resource"
            description="Latest KasprApp observation recorded by the control plane."
          />
          {!observation ? (
            <EmptyState title="No observation yet" className="py-12" />
          ) : (
            <>
              <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-5">
                <KeyValue label="App name">{observation.kaspr_app_name}</KeyValue>
                <KeyValue label="Namespace">{observation.namespace}</KeyValue>
                <KeyValue label="Resource exists">{observation.resource_exists ? 'Yes' : 'No'}</KeyValue>
                <KeyValue label="Members available">{observation.available_members ?? '—'}</KeyValue>
                <KeyValue label="Generation" mono>
                  {observation.metadata_generation ?? '—'} / {observation.observed_generation ?? '—'}
                </KeyValue>
                <KeyValue label="Rollout">
                  {observation.rollout_in_progress ? 'In progress' : 'Stable'}
                </KeyValue>
                <KeyValue label="Rebalance required">
                  {observation.rebalance_required ? 'Yes' : 'No'}
                </KeyValue>
                <KeyValue label="Rebalancing members">{observation.rebalancing_members ?? '—'}</KeyValue>
                <KeyValue label="Observed at" className="col-span-2">
                  <RelativeTime value={detail.status.last_resource_observation_time} />
                </KeyValue>
              </dl>
              <div className="grid gap-3 border-t border-[var(--color-line)] p-5 sm:grid-cols-2">
                <ConditionCard label="Ready" condition={observation.ready_condition ?? null} />
                <ConditionCard label="Progressing" condition={observation.progressing_condition ?? null} />
              </div>
            </>
          )}
        </Panel>

        <Panel>
          <PanelHeader
            title="Managed agents"
            description={
              observation
                ? `${observation.managed_agents_running ?? 0} running · ${observation.managed_agents_paused ?? 0} paused · ${observation.managed_agents_unknown ?? 0} unknown of ${observation.managed_agents_total ?? agents.length}`
                : undefined
            }
          />
          {agents.length === 0 ? (
            <EmptyState
              title="No agents reported"
              description="Agents appear once the runtime KasprApp is applied and observed."
              className="py-12"
            />
          ) : (
            <ul className="divide-y divide-[var(--color-line)]">
              {agents.map(([name, state]) => (
                <li key={name} className="flex items-center justify-between gap-4 px-5 py-3">
                  <span className="numeric truncate text-[12px]">{name}</span>
                  <span className="flex shrink-0 items-center gap-2 text-[12px] text-[var(--color-ink-muted)]">
                    <span
                      className={cn('size-1.5 rounded-full')}
                      style={{ backgroundColor: AGENT_TONE[state] ?? 'var(--color-phase-idle)' }}
                    />
                    {humanize(state)}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>

      <Panel>
        <PanelHeader
          title="Runtime binding"
          description="What the control plane last applied, and the hash it reconciles against."
        />
        {!binding ? (
          <EmptyState
            title="Not bound yet"
            description="The binding is written when apply_runtime completes."
            className="py-12"
          />
        ) : (
          <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-5 lg:grid-cols-4">
            <KeyValue label="Kaspr app">{binding.kaspr_app_name}</KeyValue>
            <KeyValue label="Namespace">{binding.namespace}</KeyValue>
            <KeyValue label="Runtime mode">{binding.runtime_mode ?? '—'}</KeyValue>
            <KeyValue label="Template version">{binding.template_version ?? '—'}</KeyValue>
            <KeyValue label={(binding.source_topics?.length ?? 0) === 1 ? 'Reads topic' : 'Reads topics'}>
              <span className="block break-all" title={(binding.source_topics ?? []).join('\n')}>
                {binding.source_topics?.length ? topicsLabel(binding.source_topics) : '—'}
              </span>
            </KeyValue>
            <KeyValue label="Applied at">
              <RelativeTime value={binding.updated_at ?? binding.created_at} />
            </KeyValue>
            <KeyValue label="Spec hash" className="col-span-2">
              {binding.last_applied_spec_hash ? (
                <Identifier value={binding.last_applied_spec_hash} truncate />
              ) : (
                '—'
              )}
            </KeyValue>
            <KeyValue label="Managed agents" className="col-span-2 lg:col-span-4">
              <span className="flex flex-wrap gap-1.5">
                {binding.managed_agent_names.map((name) => (
                  <Badge key={name}>{name}</Badge>
                ))}
              </span>
            </KeyValue>
          </dl>
        )}
      </Panel>
    </PageBody>
  );
}

function ConditionCard({
  label,
  condition,
}: {
  label: string;
  condition: CpResourceCondition | null;
}): ReactNode {
  const tone =
    condition?.status === 'True'
      ? 'running'
      : condition?.status === 'False'
        ? 'failed'
        : ('neutral' as const);

  return (
    <div className="rounded-[var(--radius-control)] border border-[var(--color-line)] p-3">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[12px] font-medium">{label}</span>
        <Badge tone={tone}>{condition?.status ?? 'Unknown'}</Badge>
      </div>
      <p className="mt-1.5 text-[12px] text-[var(--color-ink-muted)]">
        {condition?.reason ? humanize(condition.reason) : 'No reason reported'}
      </p>
      {condition?.message ? (
        <p className="mt-1 text-[12px] leading-relaxed text-[var(--color-ink-faint)]">{condition.message}</p>
      ) : null}
      {condition?.last_transition_time ? (
        <p className="mt-1.5 text-[11px] text-[var(--color-ink-faint)]">
          <RelativeTime value={condition.last_transition_time} prefix="Changed" />
        </p>
      ) : null}
    </div>
  );
}
