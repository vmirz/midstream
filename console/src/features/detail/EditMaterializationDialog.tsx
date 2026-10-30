import { useMutation, useQuery } from '@tanstack/react-query';
import { Lock } from 'lucide-react';
import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react';

import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Button, Field, Input, KeyValue } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { useMaterializationDefaults } from '@/lib/defaults';
import { displayName } from '@/lib/format';
import { useApi } from '@/lib/domains';
import { labelProblem, normalizeLabel } from '@shared/tableNames';
import { FixedTableRow, TableDraftRow } from '../datasets/TableDraftRow';
import {
  blocksSubmit,
  claimRow,
  defaultDataset,
  draftProblems,
  draftTables,
  rowProblem,
  type DraftProblem,
  type TableDraft,
} from '../datasets/tableDrafts';
import { useDatasets, useRefreshDatasets } from '../datasets/useDatasets';
import { TopicPicker, assignableMembers } from '../materializations/TopicPicker';
import { ProtectionField } from '../protection/ProtectionField';
import { choiceChanges, choiceOf, sameChoice, type ProtectionChoice } from '../protection/protection';
import { keepOrder, planTopicChange } from './topicChange';
import type {
  ConsoleAutoscaling,
  CpAccepted,
  CpAutoscale,
  CpDetail,
  CpTableSpec,
  MaterializationSummary,
  MaterializationUpdate,
  RemovedTableAction,
  TableRequest,
} from '@shared/types';

/** Used until the control plane reports its own limit. */
const FALLBACK_MAX_REPLICAS = 64;
const NO_MATERIALIZATIONS: readonly MaterializationSummary[] = [];

/**
 * Change what can change after a materialization is created: its label, the topics its runtime
 * reads, its replica count, and whether the control plane adds members while it is far behind.
 *
 * A topic added gets a table of its own, in the dataset chosen for it and named after it unless
 * renamed; its name is claimed in that dataset before the change applies. The table of a topic
 * removed is either kept, read-only, or dropped with its data - asked for each one, every time. A
 * kept table stays in its dataset, can be dropped later, and is written again if its topic is added
 * back. Adding, removing and dropping tables needs write access to their datasets. Changing the
 * topics stops the runtime until its consumer group has emptied, then starts it with the new ones.
 * A stopped materialization stays stopped and runs with the new settings once resumed.
 */
export function EditMaterializationDialog({
  open,
  onClose,
  detail,
  materializations = NO_MATERIALIZATIONS,
}: {
  open: boolean;
  onClose: () => void;
  detail: CpDetail;
  /** The fleet, so a topic another materialization reads says so. */
  materializations?: readonly MaterializationSummary[];
}): ReactNode {
  const api = useApi();
  const { materialization, status } = detail;
  const current = materialization.replicas ?? 1;
  // Absent from a control plane before 4.4, which never scales a runtime: nothing to switch.
  const currentAutoscaling = materialization.autoscaling;
  const [label, setLabel] = useState(materialization.name);
  const [replicas, setReplicas] = useState(String(current));
  const [autoscaling, setAutoscaling] = useState(currentAutoscaling ?? true);
  const [topics, setTopics] = useState<string[]>(materialization.source_topics);
  const [drafts, setDrafts] = useState<TableDraft[]>([]);
  const [rowErrors, setRowErrors] = useState<ReadonlyMap<string, DraftProblem>>(new Map());
  const [submitted, setSubmitted] = useState(false);
  const [actions, setActions] = useState<Record<string, RemovedTableAction>>({});
  const [dropKept, setDropKept] = useState<ReadonlySet<string>>(new Set());
  const [error, setError] = useState<{ field: string | null; message: string } | null>(null);
  const { defaults, datasetRules, tableRules, labelMaxLength, maxTopics, autoscaling: autoscalingPolicy } = useMaterializationDefaults();
  // Data protection (handbook 22): absent from control planes before 1.0.0-rc.5, which offer none.
  const protectionCatalog = defaults?.protection ?? null;
  const currentProtection = useMemo(() => choiceOf(materialization.protection ?? null, protectionCatalog), [materialization.protection, protectionCatalog]);
  const [protection, setProtection] = useState<ProtectionChoice>(currentProtection);
  // Then mask the rows already written (handbook 22 §22.14), purging the older snapshots unless asked not to.
  const [reprotect, setReprotect] = useState(false);
  const [purge, setPurge] = useState(true);
  const datasets = useDatasets(open);
  const refreshDatasets = useRefreshDatasets();
  const { can } = useAuth();
  const toast = useToast();

  // Start from the live values each time the dialog opens, not from a previous edit.
  useEffect(() => {
    if (open) {
      setLabel(materialization.name);
      setReplicas(String(current));
      setAutoscaling(currentAutoscaling ?? true);
      setTopics(materialization.source_topics);
      setDrafts([]);
      setRowErrors(new Map());
      setSubmitted(false);
      setActions({});
      setDropKept(new Set());
      setError(null);
      setProtection(currentProtection);
      setReprotect(false);
      setPurge(true);
    }
    // The topics are read when the dialog opens; a change while it is open is not a new edit.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, current, currentAutoscaling]);

  const change = useMemo(() => planTopicChange(detail, topics), [detail, topics]);
  // Every name the materialization's tables hold - written, kept, being dropped, dropped with their
  // files kept - which no new table may take.
  const existingNames = useMemo(() => [...change.fixed.values(), ...change.taken], [change]);

  const chooseTopics = (next: string[]): void => {
    const ordered = keepOrder(materialization.source_topics, next);
    const added = ordered.filter((topic) => !change.fixed.has(topic));
    setTopics(ordered);
    setDrafts((existing) =>
      draftTables(added, existing, { rules: tableRules, dataset: defaultDataset(existing, datasets.data?.datasets ?? null), taken: existingNames }),
    );
  };

  const changeTable = (draft: TableDraft): void => {
    setDrafts((existing) =>
      draftTables(
        existing.map((candidate) => candidate.topic),
        existing.map((candidate) => (candidate.topic === draft.topic ? draft : candidate)),
        { rules: tableRules, dataset: '', taken: existingNames },
      ),
    );
    setRowErrors((errors) => {
      if (!errors.has(draft.topic)) return errors;
      const next = new Map(errors);
      next.delete(draft.topic);
      return next;
    });
  };

  const problems = useMemo(
    () => draftProblems(drafts, { rules: tableRules, datasets: datasets.data?.datasets ?? null, taken: existingNames }),
    [drafts, tableRules, datasets.data, existingNames],
  );
  const problemOf = (draft: TableDraft, position: number): DraftProblem | null => rowErrors.get(draft.topic) ?? problems[position] ?? null;
  const draftsBlocked = drafts.some((draft, position) => blocksSubmit(problemOf(draft, position)));
  const labelError = labelProblem(label, labelMaxLength);
  const labelChanged = normalizeLabel(label) !== normalizeLabel(materialization.name) && !(normalizeLabel(label) === '' && materialization.name === materialization.materialization_id);
  const others = useMemo(
    () => materializations.filter((item) => item.materialization_id !== materialization.materialization_id),
    [materializations, materialization.materialization_id],
  );

  // Shared with the topic picker, so this is usually answered from cache.
  const catalog = useQuery({ queryKey: ['topics'], queryFn: () => api.topics(), staleTime: 15_000, enabled: open });
  // Every topic's partition count, once all are known: what the members share between them.
  const counts = topics.map((name) => catalog.data?.topics.find((topic) => topic.name === name)?.partitions ?? null);
  const partitions = counts.every((count): count is number => count !== null) ? counts : null;

  const maxReplicas = defaults?.maxReplicas ?? FALLBACK_MAX_REPLICAS;
  const parsed = /^\d+$/.test(replicas.trim()) ? Number(replicas.trim()) : null;
  const invalidReplicas =
    parsed === null || parsed < 1
      ? 'A whole number of 1 or more'
      : parsed > maxReplicas
        ? `At most ${maxReplicas} on this control plane`
        : null;
  const replicasChanged = parsed !== null && parsed !== current;
  const autoscalingChanged = currentAutoscaling !== undefined && autoscaling !== currentAutoscaling;
  const undecided = change.removed.filter((spec) => !actions[spec.name]);
  const droppedKept = change.stillKept.filter((spec) => dropKept.has(spec.name));
  const protectionChanged = protectionCatalog !== null && !sameChoice(protection, currentProtection);
  // Choosing how it masks needs Choose data protection on every dataset it writes into, those of the
  // topics this edit adds or writes again included: the new policy masks their tables too.
  const datasetIdOf = (name: string): string | null => datasets.data?.datasets.find((item) => item.name === name)?.dataset_id ?? null;
  const protectionLockedBy = [
    ...new Set(
      [
        ...[...materialization.tables, ...change.resumed].map((spec) => ({ id: spec.dataset_id, name: spec.dataset })),
        ...drafts.filter((draft) => draft.dataset).map((draft) => ({ id: datasetIdOf(draft.dataset), name: draft.dataset })),
      ]
        .filter((item) => item.id !== null && !can('dataset.protect', item.id))
        .map((item) => item.name),
    ),
  ];
  // Rewriting what is already written is table maintenance, of every table it keeps.
  const mayReprotect =
    can('materializations.maintain') &&
    [...materialization.tables, ...(materialization.retired_tables ?? [])].every((spec) => can('dataset.protect', spec.dataset_id));
  const reprotectAfter = protectionChanged && mayReprotect && protectionLockedBy.length === 0 && reprotect;
  const anyChange =
    labelChanged || replicasChanged || autoscalingChanged || change.changesTopics || droppedKept.length > 0 || protectionChanged;
  const stopped = status.desired_phase === 'stopped';
  // Each table added, written again, removed or dropped is in a dataset the person must be able to write.
  const affected = [...change.resumed, ...change.removed, ...droppedKept];
  const unwritable = affected.filter((spec) => !can('dataset.write', spec.dataset_id));
  const datasetNames = new Set([...affected.map((spec) => spec.dataset), ...drafts.map((draft) => draft.dataset).filter(Boolean)]);
  const datasetsAffected = [...datasetNames].sort((a, b) => a.localeCompare(b));
  const pending = materialization.pending_topic_change ?? null;
  const blocked =
    Boolean(invalidReplicas) ||
    Boolean(labelError) ||
    topics.length === 0 ||
    undecided.length > 0 ||
    !anyChange ||
    draftsBlocked ||
    unwritable.length > 0 ||
    (protectionChanged && protectionLockedBy.length > 0) ||
    (pending !== null && (change.changesTopics || droppedKept.length > 0));

  const mutation = useMutation({
    mutationFn: (body: MaterializationUpdate) => api.update(materialization.materialization_id, body),
    onSuccess: (accepted) => {
      toast.success(
        'Change requested',
        describeAccepted(materialization, accepted, {
          current,
          replicas: parsed,
          autoscaling: autoscalingChanged ? autoscaling : null,
          stopped,
          protection: protectionChanged,
          reprotect: reprotectAfter,
        }),
      );
      void refreshDatasets();
      onClose();
    },
    onError: (failure: unknown, body) => {
      const message = failure instanceof ApiError ? failure.message : 'The control plane rejected the change.';
      const row = failure instanceof ApiError ? claimRow(failure.details) : null;
      const topic = row ? body.tables?.[row.index]?.source_topic : undefined;
      if (row && topic && drafts.some((draft) => draft.topic === topic)) {
        setRowErrors(new Map([[topic, rowProblem(row.field, message)]]));
        setError(null);
        void refreshDatasets();
      } else if (row?.field === 'source_topic') {
        // A kept table's topic, read again, has no row of its own: said under the topics.
        setError({ field: 'tables', message });
      } else {
        const field = failure instanceof ApiError && typeof failure.details.field === 'string' ? failure.details.field : null;
        setError({ field, message });
      }
      toast.error('Could not change the materialization', message);
    },
  });

  const onSubmit = (event: FormEvent): void => {
    event.preventDefault();
    setSubmitted(true);
    if (blocked || parsed === null) return;
    const body: MaterializationUpdate = {};
    if (labelChanged) body.name = normalizeLabel(label);
    if (replicasChanged) body.replicas = parsed;
    if (autoscalingChanged) body.autoscaling = autoscaling;
    if (change.changesTopics) {
      const byTopic = new Map(drafts.map((draft) => [draft.topic, draft]));
      body.tables = topics.map((topic): TableRequest => {
        const draft = byTopic.get(topic);
        return draft ? { source_topic: topic, dataset: draft.dataset, name: draft.name.trim() } : { source_topic: topic };
      });
    }
    const removed: Record<string, RemovedTableAction> = {};
    for (const spec of change.removed) removed[spec.name] = actions[spec.name] as RemovedTableAction;
    for (const spec of droppedKept) removed[spec.name] = 'drop';
    if (Object.keys(removed).length > 0) body.removed_tables = removed;
    if (protectionChanged) body.protection = { kinds: protection, ...(reprotectAfter ? { reprotect: { purge } } : {}) };
    setError(null);
    mutation.mutate(body);
  };
  const specOf = (topic: string): CpTableSpec | undefined =>
    [...materialization.tables, ...(materialization.retired_tables ?? [])].find((spec) => spec.source_topic === topic);

  const errorFor = (field: string): string | null => (error && (error.field === field || (field === 'replicas' && !error.field)) ? error.message : null);

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Edit ${displayName(materialization)}`}
      description="Change its name, the topics it reads and its scale. A change of topics or scale re-applies the runtime."
      footer={
        <>
          <Button variant="ghost" type="button" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="edit-materialization" disabled={blocked} loading={mutation.isPending}>
            Save
          </Button>
        </>
      }
    >
      <form id="edit-materialization" onSubmit={onSubmit} className="space-y-5">
        <Field
          label="Name"
          hint="A label for people, not unique. Left empty, it goes by its id. Changing it touches nothing else."
          error={errorFor('name') ?? labelError}
        >
          <Input
            value={label}
            onChange={(event) => setLabel(event.target.value)}
            placeholder={materialization.materialization_id}
            autoComplete="off"
            maxLength={labelMaxLength + 16}
          />
        </Field>

        {pending ? (
          <p className="rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2 text-[12px] text-[var(--color-ink-muted)]">
            Adding {pending.added.map((spec) => `${spec.dataset}.${spec.name}`).join(', ')}: their names are being claimed. The
            topics can be changed again once that is done.
          </p>
        ) : null}

        <TopicPicker
          label="Source topics"
          value={topics}
          onChange={chooseTopics}
          maxTopics={maxTopics}
          materializations={others}
          renderTable={(topic) => {
            const spec = specOf(topic);
            if (spec) return <FixedTableRow dataset={spec.dataset} name={spec.name} note={change.notes.get(topic)} />;
            const position = drafts.findIndex((draft) => draft.topic === topic);
            const draft = drafts[position];
            if (!draft) return null;
            const problem = problemOf(draft, position);
            return (
              <TableDraftRow
                draft={draft}
                problem={problem}
                showProblem={submitted || rowErrors.has(topic) || (problem !== null && (problem.field === 'name' || draft.dataset !== ''))}
                datasetRules={datasetRules}
                onChange={changeTable}
              />
            );
          }}
          notes={new Map([...change.notes].filter(([topic]) => !specOf(topic)))}
          verb="saving"
          error={errorFor('tables') ?? errorFor('source_topics') ?? (topics.length === 0 ? 'A materialization reads at least one topic' : null)}
        />

        {change.removed.length > 0 ? (
          <fieldset className="space-y-2 rounded-[var(--radius-control)] border border-[var(--color-line)] px-3 py-3">
            <legend className="px-1 text-[13px] font-medium">
              {change.removed.length === 1 ? 'The removed topic’s table' : 'The removed topics’ tables'}
            </legend>
            <p className="text-[12px] text-[var(--color-ink-muted)]">
              Keep a table to go on reading it, read-only; add its topic back later to write it again. Dropping deletes
              the table and its data once the runtime has stopped reading the topic, and cannot be undone.
            </p>
            {change.removed.map((spec) => (
              <RemovedTableChoice
                key={spec.name}
                spec={spec}
                writable={can('dataset.write', spec.dataset_id)}
                value={actions[spec.name] ?? null}
                onChange={(action) => setActions((existing) => ({ ...existing, [spec.name]: action }))}
              />
            ))}
            {errorFor('removed_tables') ? (
              <p className="text-[12px] text-[var(--color-phase-failed)]">{errorFor('removed_tables')}</p>
            ) : null}
          </fieldset>
        ) : null}

        {change.stillKept.length > 0 ? (
          <fieldset className="space-y-2 rounded-[var(--radius-control)] border border-[var(--color-line)] px-3 py-3">
            <legend className="px-1 text-[13px] font-medium">Kept tables</legend>
            <p className="text-[12px] text-[var(--color-ink-muted)]">
              Read-only since their topics were removed. Add a topic back above to write its table again, or drop a
              table with its data.
            </p>
            {change.stillKept.map((spec) => (
              <label key={spec.name} className="flex cursor-pointer items-center gap-2 text-[13px]">
                <input
                  type="checkbox"
                  checked={dropKept.has(spec.name)}
                  onChange={(event) =>
                    setDropKept((existing) => {
                      const next = new Set(existing);
                      if (event.target.checked) next.add(spec.name);
                      else next.delete(spec.name);
                      return next;
                    })
                  }
                  disabled={!can('dataset.write', spec.dataset_id)}
                  className="size-3.5 cursor-pointer accent-[var(--color-phase-failed)] disabled:cursor-not-allowed"
                />
                <span>
                  Drop <code className="numeric">{qualified(spec)}</code> and its data
                  <span className="text-[var(--color-ink-faint)]"> · topic {spec.source_topic}</span>
                  {can('dataset.write', spec.dataset_id) ? null : <NoWriteAccess dataset={spec.dataset} />}
                </span>
              </label>
            ))}
          </fieldset>
        ) : null}

        {unwritable.length > 0 ? (
          <p className="flex items-start gap-1.5 text-[12px] text-[var(--color-phase-failed)]">
            <Lock className="mt-0.5 size-3.5 shrink-0" aria-hidden />
            <span>
              {unwritable.map(qualified).join(', ')} {unwritable.length === 1 ? 'is' : 'are'} in{' '}
              {[...new Set(unwritable.map((spec) => spec.dataset))].join(' and ')}, which you cannot change: ask one of its
              owners for write access.
            </span>
          </p>
        ) : null}

        {change.changesTopics || droppedKept.length > 0 ? (
          <p className="rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2 text-[12px] leading-relaxed text-[var(--color-ink-muted)]">
            {datasetsAffected.length > 0
              ? `Changes tables in ${datasetsAffected.length === 1 ? 'dataset' : 'datasets'} ${datasetsAffected.join(', ')}. `
              : ''}
            {change.added.length > 0
              ? `${change.added.length === 1 ? 'The new table’s name is' : 'The new tables’ names are'} claimed first, then ${change.added.length === 1 ? 'its topic is' : 'their topics are'} sampled and ${change.added.length === 1 ? 'its table' : 'their tables'} created. `
              : ''}
            {stopped
              ? 'It is stopped: it reads the new set of topics once resumed.'
              : 'Then the runtime stops until its consumer group has emptied - about two minutes - and starts again with the new set of topics, each where its table left off.'}
          </p>
        ) : null}

        <Field
          label="Replicas"
          hint={replicaHint({ parsed, current, partitions, topics, stopped })}
          error={errorFor('replicas') ?? (replicas.trim() && invalidReplicas ? invalidReplicas : null)}
        >
          <Input
            value={replicas}
            onChange={(event) => setReplicas(event.target.value)}
            inputMode="numeric"
            spellCheck={false}
            aria-describedby="replicas-current"
            className="numeric max-w-[10rem]"
          />
        </Field>

        {currentAutoscaling !== undefined ? (
          <label className="flex items-start gap-2.5">
            <input
              type="checkbox"
              checked={autoscaling}
              onChange={(event) => setAutoscaling(event.target.checked)}
              className="mt-[3px] size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
            />
            <span className="space-y-1.5">
              <span className="block text-[13px] font-medium">Autoscale</span>
              <span className="block text-[12px] text-[var(--color-ink-muted)]">
                {autoscaleHint({ on: autoscaling, autoscale: detail.autoscale ?? null, policy: autoscalingPolicy })}
              </span>
            </span>
          </label>
        ) : null}

        {protectionCatalog ? (
          <ProtectionField
            idPrefix="edit-protection"
            catalog={protectionCatalog}
            value={protection}
            defaultChoice={currentProtection}
            baselineTag="current"
            resetLabel="Keep the current protection"
            lockedBy={protectionLockedBy}
            error={error?.field?.startsWith('protection') ? error.message : null}
            onChange={setProtection}
            startOpen={protectionChanged}
            note={
              protectionChanged ? (
                <>
                  {choiceChanges(currentProtection, protection, protectionCatalog).join('. ')}. Applies to records written
                  from now on, within seconds, without a restart. Rows already written keep their values
                  {mayReprotect && protectionLockedBy.length === 0 ? ' unless masked as well' : ''}.
                </>
              ) : (
                'Applies to records written from now on, within seconds, without a restart. Rows already written keep their values.'
              )
            }
          />
        ) : null}

        {protectionChanged && mayReprotect && protectionLockedBy.length === 0 ? (
          <div className="space-y-2.5">
            <label className="flex items-start gap-2.5">
              <input
                type="checkbox"
                checked={reprotect}
                onChange={(event) => setReprotect(event.target.checked)}
                className="mt-[3px] size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
              />
              <span className="space-y-1.5">
                <span className="block text-[13px] font-medium">Also mask rows already written</span>
                <span className="block text-[12px] text-[var(--color-ink-muted)]">
                  Once each table records the new policy, the maintenance app rewrites the files holding in plain text what it now
                  masks. Values masked before stay as they were.
                </span>
              </span>
            </label>
            {reprotect ? (
              <label className="ml-6 flex items-start gap-2.5">
                <input
                  type="checkbox"
                  checked={purge}
                  onChange={(event) => setPurge(event.target.checked)}
                  className="mt-[3px] size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
                />
                <span className="space-y-1.5">
                  <span className="block text-[13px] font-medium">Purge the plain text now</span>
                  <span className="block text-[12px] text-[var(--color-ink-muted)]">
                    Then expire each table’s older snapshots and delete the files they alone held: time travel to before the
                    rewrite ends. Without it, the old files stay readable through older snapshots until they expire.
                  </span>
                </span>
              </label>
            ) : null}
          </div>
        ) : null}

        <div className="space-y-2 rounded-[var(--radius-control)] border border-[var(--color-line)] px-3 py-3">
          <p className="text-[12px] text-[var(--color-ink-muted)]">Fixed when the materialization was created:</p>
          <dl className="grid gap-3 sm:grid-cols-2">
            <KeyValue label="Id" mono>
              {materialization.materialization_id}
            </KeyValue>
            <KeyValue label="Storage per replica" mono>
              {materialization.storage_size ?? '—'}
            </KeyValue>
            <KeyValue label="Destination" mono className="sm:col-span-2">
              <span title={materialization.destination_uri}>{materialization.destination_uri}</span>
            </KeyValue>
          </dl>
        </div>
      </form>
    </Dialog>
  );
}

/** A table as SQL reads it: `<dataset>.<table>`. */
function qualified(spec: Pick<CpTableSpec, 'dataset' | 'name'>): string {
  return `${spec.dataset}.${spec.name}`;
}

function NoWriteAccess({ dataset }: { dataset: string }): ReactNode {
  return (
    <span className="ml-1 inline-flex items-center gap-1 text-[var(--color-ink-faint)]" title={`Needs write access to ${dataset}`}>
      <Lock className="size-3" aria-hidden />
      no write access to {dataset}
    </span>
  );
}

function RemovedTableChoice({
  spec,
  writable,
  value,
  onChange,
}: {
  spec: CpTableSpec;
  /** Whether the person may change the table's dataset; removing its topic does. */
  writable: boolean;
  value: RemovedTableAction | null;
  onChange: (action: RemovedTableAction) => void;
}): ReactNode {
  const name = `removed-${spec.name}`;
  const option = (action: RemovedTableAction, label: string): ReactNode => (
    <label
      className={cn(
        'flex cursor-pointer items-center gap-1.5 rounded-md border px-2 py-1 text-[12px]',
        value === action
          ? action === 'drop'
            ? 'border-[var(--color-phase-failed)] text-[var(--color-phase-failed)]'
            : 'border-[var(--color-accent)] text-[var(--color-ink)]'
          : 'border-[var(--color-line)] text-[var(--color-ink-muted)]',
      )}
    >
      <input
        type="radio"
        name={name}
        value={action}
        checked={value === action}
        disabled={!writable}
        onChange={() => onChange(action)}
        className="sr-only"
      />
      {label}
    </label>
  );
  return (
    <div role="radiogroup" aria-label={`What becomes of ${spec.name}`} className="flex flex-wrap items-center justify-between gap-2">
      <span className="min-w-0 text-[13px]">
        <code className="numeric">{qualified(spec)}</code>
        <span className="text-[var(--color-ink-faint)]"> · topic {spec.source_topic}</span>
        {writable ? null : <NoWriteAccess dataset={spec.dataset} />}
      </span>
      <span className="flex items-center gap-1.5">
        {option('keep', 'Keep read-only')}
        {option('drop', 'Drop with its data')}
      </span>
    </div>
  );
}

/** The toast after a change is accepted: what it does, as the control plane planned it. */
function describeAccepted(
  materialization: CpDetail['materialization'],
  accepted: CpAccepted,
  {
    current,
    replicas,
    autoscaling,
    stopped,
    protection = false,
    reprotect = false,
  }: { current: number; replicas: number | null; autoscaling: boolean | null; stopped: boolean; protection?: boolean; reprotect?: boolean },
): string {
  const parts: string[] = [];
  const change = accepted.topic_change;
  const list = (specs: CpTableSpec[]): string => specs.map(qualified).join(', ');
  if (change?.added.length) parts.push(`adding ${list(change.added)}`);
  if (change?.resumed.length) parts.push(`writing ${list(change.resumed)} again`);
  if (change?.kept.length) parts.push(`keeping ${list(change.kept)} read-only`);
  if (change?.dropped.length) parts.push(`dropping ${list(change.dropped)}`);
  if (replicas !== null && replicas !== current) parts.push(`${current} → ${replicas} replica${replicas === 1 ? '' : 's'}`);
  if (autoscaling !== null) parts.push(autoscaling ? 'autoscaling on' : 'autoscaling off');
  if (protection) {
    parts.push(
      reprotect
        ? 'new data protection, for records written from now on and then for the rows already written'
        : 'new data protection, for records written from now on',
    );
  }
  if (accepted.name && accepted.name !== materialization.name) parts.push(`now named ${accepted.name}`);
  const what = parts.length ? parts.join('; ') : 'no change';
  return `${displayName(materialization)}: ${what}${stopped ? ', applied when it is resumed' : ''}. Watch the phase for convergence.`;
}

/**
 * What the autoscale switch does for this materialization: what turning it off undoes now, and the
 * deployment's own switch, which overrides it.
 */
function autoscaleHint({
  on,
  autoscale,
  policy,
}: {
  on: boolean;
  autoscale: CpAutoscale | null;
  policy: ConsoleAutoscaling | null;
}): string {
  if (policy && !policy.enabled) {
    return 'Autoscaling is off for every materialization (Settings › Materializations), so nothing is scaled while it is off there.';
  }
  const scaled = autoscale?.replicas ?? null;
  if (scaled !== null && autoscale) {
    return on
      ? `Running ${scaled} members while it catches up; back to ${autoscale.own_replicas} once caught up.`
      : `Running ${scaled} members while it catches up. Turned off, it goes back to ${autoscale.own_replicas} at once.`;
  }
  const most = policy ? `, up to ${policy.maxReplicas},` : '';
  return `Adds members while it is far behind its topics${most} and goes back to its replicas once caught up.`;
}

/**
 * What a replica count means for these topics. Members share every topic's partitions, but the
 * partitions of topics with as many partitions go to the same members, so the members that can be
 * kept busy are fewer than the partitions in all (`assignableMembers`).
 */
function replicaHint({
  parsed,
  current,
  partitions,
  topics,
  stopped,
}: {
  parsed: number | null;
  current: number;
  partitions: number[] | null;
  topics: readonly string[];
  stopped: boolean;
}): ReactNode {
  const parts: string[] = [`Currently ${current}.`];
  if (partitions !== null && topics.length > 0) {
    const total = partitions.reduce((sum, count) => sum + count, 0);
    const busy = assignableMembers(partitions);
    const what = topics.length === 1 ? `${topics[0]} has ${total} partition${total === 1 ? '' : 's'}` : `Its ${topics.length} topics have ${total} partitions`;
    parts.push(
      parsed !== null && parsed > busy
        ? `${what}, which keep at most ${busy} member${busy === 1 ? '' : 's'} busy, so ${parsed - busy} would have none to read.`
        : `${what}, shared among the members.`,
    );
  }
  if (stopped) parts.push('It is stopped, so the new count applies when it is resumed.');
  return <span id="replicas-current">{parts.join(' ')}</span>;
}
