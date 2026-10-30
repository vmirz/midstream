import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { RefreshCw, Shield, ShieldOff } from 'lucide-react';
import type { ReactNode } from 'react';

import { PageBody } from '@/components/AppShell';
import { RelativeTime } from '@/components/Inline';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, EmptyState, KeyValue, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { useMaterializationDefaults } from '@/lib/defaults';
import { formatExact } from '@/lib/format';
import { useApi } from '@/lib/domains';
import type { CpProtectionCatalog, CpProtectionReviewItem, CpProtectionReviewTable, DetailSnapshot } from '@shared/types';

import { ProtectionChips, ProtectionStateBadge } from './ProtectionSummary';
import { describeChoice, choiceOf, kindLabel, maskName } from './protection';
import { ReprotectPanel } from './ReprotectPanel';

/**
 * The Protection tab (handbook 22): the policy in force and how far it has reached; what the tables
 * hold in plain text that they mask now, with the action that masks it (a reprotect); per table, every
 * protected field with its kind, its mask and the schema it was protected from; what the current
 * rules would protect otherwise, with the action that applies them; and the history of policies.
 */
export function ProtectionTab({ snapshot }: { snapshot: DetailSnapshot }): ReactNode {
  const api = useApi();
  const materializationId = snapshot.materializationId;
  const catalog = useMaterializationDefaults().defaults?.protection ?? null;
  const review = useQuery({
    queryKey: ['protection', materializationId],
    queryFn: () => api.protection(materializationId),
    refetchInterval: 15_000,
  });
  const detail = snapshot.detail;
  const { can } = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const written = detail?.materialization.tables ?? [];
  const mayChoose = can('materializations.edit') && written.every((spec) => can('dataset.protect', spec.dataset_id));
  const applyRules = useMutation({
    mutationFn: () =>
      api.update(materializationId, {
        protection: { kinds: choiceOf(detail?.materialization.protection ?? null, catalog), apply_rules: true },
      }),
    onSuccess: () => {
      toast.success('Applying the current rules', 'Each table records them within seconds, and its writers follow.');
      void queryClient.invalidateQueries({ queryKey: ['protection', materializationId] });
    },
    onError: (error: unknown) => toast.error('Could not apply the current rules', error instanceof ApiError ? error.message : String(error)),
  });

  if (review.isLoading) {
    return (
      <PageBody className="space-y-4">
        <Skeleton className="h-28" />
        <Skeleton className="h-64" />
      </PageBody>
    );
  }
  if (review.error || !review.data) {
    return (
      <PageBody>
        <Panel>
          <EmptyState
            title="Protection could not be loaded"
            description={review.error instanceof Error ? review.error.message : 'The control plane did not answer.'}
            action={
              <Button size="sm" onClick={() => void review.refetch()}>
                Retry
              </Button>
            }
          />
        </Panel>
      </PageBody>
    );
  }
  const data = review.data;
  // A table that has not recorded the current revision yet lists what that revision is about to
  // change; one that has lists only what a newer release's rules match that it recorded otherwise.
  const behind = (table: CpProtectionReviewTable): boolean => table.created && (table.recorded_revision ?? 0) < data.revision;
  const pending = data.review.filter(behind);
  const ruled = data.review.filter((table) => !behind(table));
  const pendingCount = pending.reduce((count, table) => count + table.would_change.length, 0);
  const rulesWouldChange = ruled.reduce((count, table) => count + table.would_change.length, 0);
  return (
    <PageBody className="space-y-4">
      <Panel>
        <PanelHeader
          title="Data protection"
          description="Sensitive values are masked before they are written: no query, engine or file ever holds them in plain text."
          actions={data.revision > 0 ? <ProtectionStateBadge summary={data} /> : null}
        />
        <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-5">
          <KeyValue label="Policy" className="col-span-2">
            <ProtectionChips policy={data.policy} catalog={catalog} />
          </KeyValue>
          <KeyValue label="Revision">{data.revision > 0 ? data.revision : 'None: created before data protection'}</KeyValue>
          <KeyValue label="Chosen by">
            {data.changed_by ?? '—'}
            {data.reason === 'apply_rules' ? <span className="text-[var(--color-ink-muted)]"> · current rules applied</span> : null}
          </KeyValue>
          <KeyValue label="Members on it">
            {data.members_reporting === 0 ? '—' : `${formatExact(data.members_reporting - data.members_behind.length)} of ${formatExact(data.members_reporting)}`}
          </KeyValue>
          <KeyValue label="Tables recording it">
            {formatExact(Object.keys(data.tables).length - data.tables_behind.length)} of {formatExact(Object.keys(data.tables).length)}
          </KeyValue>
        </dl>
        <p className="border-t border-[var(--color-line)] px-5 py-3 text-[12px] text-[var(--color-ink-muted)]">
          A change applies to records written from then on, within seconds and without a restart. Rows already written keep
          the values they were written with until a reprotect masks them.
        </p>
      </Panel>

      <ReprotectPanel review={data} detail={detail ?? null} />

      {pendingCount > 0 ? (
        <Panel>
          <PanelHeader
            title={`${formatExact(pendingCount)} ${pendingCount === 1 ? 'field' : 'fields'} revision ${data.revision} is about to protect otherwise`}
            description="Each table records the revision on its next commit, within seconds; its writers follow."
          />
          <ReviewItems tables={pending} catalog={catalog} />
        </Panel>
      ) : null}

      {rulesWouldChange > 0 ? (
        <Panel>
          <PanelHeader
            title={`${formatExact(rulesWouldChange)} ${rulesWouldChange === 1 ? 'field' : 'fields'} the current rules would protect otherwise`}
            description="A newer release's rules match fields these tables recorded otherwise. A field keeps the protection its table recorded until the rules are applied again; applying them affects records written from then on."
            actions={
              mayChoose ? (
                <Button size="sm" onClick={() => applyRules.mutate()} loading={applyRules.isPending}>
                  <RefreshCw className="size-3.5" />
                  Apply the current rules
                </Button>
              ) : null
            }
          />
          <ReviewItems tables={ruled} catalog={catalog} />
        </Panel>
      ) : null}

      {data.review.map((table) => (
        <TableProtectionPanel key={table.table} table={table} catalog={catalog} counters={data.tables[table.table]} revision={data.revision} />
      ))}
      {data.hidden_tables > 0 ? (
        <p className="text-[12px] text-[var(--color-ink-muted)]">
          {data.hidden_tables} {data.hidden_tables === 1 ? 'table is' : 'tables are'} in datasets whose schemas you may not see.
        </p>
      ) : null}
    </PageBody>
  );
}

/** Each field whose protection would change, with its table: `kind mask → kind mask`. */
function ReviewItems({ tables, catalog }: { tables: CpProtectionReviewTable[]; catalog: CpProtectionCatalog | null }): ReactNode {
  const describe = (decision: CpProtectionReviewItem['current']): string =>
    decision ? `${kindLabel(catalog, decision.kind)} ${maskName(decision.mask, catalog).toLowerCase()}` : 'plain';
  return (
    <ul className="divide-y divide-[var(--color-line)]">
      {tables.flatMap((table) =>
        table.would_change.map((item) => (
          <li key={`${table.table}:${item.field_id}`} className="flex flex-wrap items-center justify-between gap-2 px-5 py-2.5 text-[13px]">
            <code className="numeric">
              {table.dataset}.{table.table} · {item.name}
            </code>
            <span className="text-[12px] text-[var(--color-ink-muted)]">
              {describe(item.current)}
              {' → '}
              {describe(item.proposed)}
            </span>
          </li>
        )),
      )}
    </ul>
  );
}

function TableProtectionPanel({
  table,
  catalog,
  counters,
  revision,
}: {
  table: CpProtectionReviewTable;
  catalog: CpProtectionCatalog | null;
  counters: { recorded_revision: number | null; protected_values: number; withheld_values: number } | undefined;
  revision: number;
}): ReactNode {
  const active = table.fields.filter((field) => field.active);
  const plainAgain = table.fields.filter((field) => !field.active);
  return (
    <Panel>
      <PanelHeader
        title={`${table.dataset}.${table.table}`}
        description={
          !table.created
            ? 'Not created yet.'
            : `${formatExact(active.length)} protected ${active.length === 1 ? 'field' : 'fields'} · ${formatExact(counters?.protected_values ?? 0)} values masked and ${formatExact(counters?.withheld_values ?? 0)} withheld so far`
        }
        actions={
          table.created && (table.recorded_revision ?? 0) < revision ? (
            <Badge tone="pending">Recording revision {revision}</Badge>
          ) : table.created ? (
            <Badge tone="neutral">Revision {table.recorded_revision}</Badge>
          ) : null
        }
      />
      {active.length === 0 && plainAgain.length === 0 ? (
        <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">
          {table.created ? 'No field is protected: no field name matches a kind the policy masks.' : '—'}
        </p>
      ) : (
        <table className="w-full text-[13px]">
          <thead>
            <tr className="border-b border-[var(--color-line)] text-left text-[11px] uppercase tracking-wide text-[var(--color-ink-faint)]">
              <th className="px-5 py-2 font-medium">Field</th>
              <th className="px-3 py-2 font-medium">Kind</th>
              <th className="px-3 py-2 font-medium">Mask</th>
              <th className="px-5 py-2 text-right font-medium">Since schema</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--color-line)]">
            {active.map((field) => (
              <tr key={field.field_id}>
                <td className="px-5 py-2">
                  <span className="inline-flex items-center gap-1.5">
                    <Shield className="size-3.5 text-[var(--color-accent)]" aria-label="protected" />
                    <code className="numeric">{field.name}</code>
                  </span>
                </td>
                <td className="px-3 py-2">{kindLabel(catalog, field.kind)}</td>
                <td className="px-3 py-2">{maskName(field.mask, catalog)}</td>
                <td className="numeric px-5 py-2 text-right">{field.since_schema_id}</td>
              </tr>
            ))}
            {plainAgain.map((field) => (
              <tr key={field.field_id} className="text-[var(--color-ink-muted)]">
                <td className="px-5 py-2">
                  <span className="inline-flex items-center gap-1.5">
                    <ShieldOff className="size-3.5" aria-label="written plain again" />
                    <code className="numeric">{field.name}</code>
                  </span>
                </td>
                <td className="px-3 py-2">{kindLabel(catalog, field.kind)}</td>
                <td className="px-3 py-2">Plain since schema {field.until_schema_id}</td>
                <td className="numeric px-5 py-2 text-right">{field.since_schema_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {table.history.length > 0 ? (
        <div className="border-t border-[var(--color-line)] px-5 py-3">
          <p className="mb-1.5 text-[11px] font-medium uppercase tracking-wide text-[var(--color-ink-faint)]">History</p>
          <ol className="space-y-1 text-[12px] text-[var(--color-ink-muted)]">
            {[...table.history].reverse().map((change) => (
              <li key={`${change.revision}:${change.at ?? ''}`}>
                <span className="text-[var(--color-ink)]">Revision {change.revision}</span>
                {change.at ? (
                  <>
                    {' · '}
                    <RelativeTime value={change.at} />
                  </>
                ) : null}
                {change.by ? ` · ${change.by}` : ''}
                {change.schema_id !== null ? ` · schema ${change.schema_id}` : ''}
                {' · '}
                {describeChoice(choiceOf(change.policy, catalog), catalog)}
                {change.reason === 'apply_rules' ? ' (current rules applied)' : ''}
              </li>
            ))}
          </ol>
        </div>
      ) : null}
    </Panel>
  );
}
