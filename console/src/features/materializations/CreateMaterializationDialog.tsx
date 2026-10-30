import { useMutation } from '@tanstack/react-query';
import { ChevronRight } from 'lucide-react';
import { useMemo, useState, type FormEvent, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';

import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Button, Field, Input, Select } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { ApiError, type CreateMaterializationInput } from '@/lib/api';
import { useMaterializationDefaults } from '@/lib/defaults';
import { useApi, useDomainPath } from '@/lib/domains';
import { labelProblem } from '@shared/tableNames';
import type { MaterializationSummary, PartitionGranularity } from '@shared/types';

import { ProtectionField } from '../protection/ProtectionField';
import { choiceOf, sameChoice, type ProtectionChoice } from '../protection/protection';
import { TableDraftRow } from '../datasets/TableDraftRow';
import {
  blocksSubmit,
  claimRow,
  defaultDataset,
  draftProblems,
  draftTables,
  rowProblem,
  tableRequests,
  type DraftProblem,
  type TableDraft,
} from '../datasets/tableDrafts';
import { useDatasets, useRefreshDatasets } from '../datasets/useDatasets';
import { TopicPicker } from './TopicPicker';

interface FormState {
  /** A label: free text; empty, the materialization goes by its id. */
  name: string;
  /** In the order chosen: one table each. */
  source_topics: string[];
  /** Each topic's table: the dataset it goes in, and its name. */
  tables: TableDraft[];
  replicas: string;
  /** On unless turned off: more members while its runtime is far behind. */
  autoscaling: boolean;
  labels: string;
  skip_tombstones: boolean;
  /** Empty takes the control plane's default. */
  partition_granularity: PartitionGranularity | '';
  /** Null until changed: the domain's Protected by default (handbook 22). */
  protection: ProtectionChoice | null;
}

/** Shown until the control plane reports its own defaults, and when it predates reporting them. */
const FALLBACK_REPLICAS = 1;
const ADVANCED_FIELDS = new Set(['replicas', 'autoscaling', 'labels', 'skip_tombstones', 'partition_granularity']);

const NO_MATERIALIZATIONS: readonly MaterializationSummary[] = [];

const EMPTY: FormState = {
  name: '',
  source_topics: [],
  tables: [],
  replicas: '',
  autoscaling: true,
  labels: '',
  skip_tombstones: false,
  partition_granularity: '',
  protection: null,
};

const GRANULARITY_LABELS: Record<PartitionGranularity, string> = {
  hour: 'By hour',
  day: 'By day',
  month: 'By month',
  none: 'Not partitioned',
};

/** Parse `key=value, key2=value2` into a label map. */
function parseLabels(raw: string): Record<string, string> {
  const entries = raw
    .split(',')
    .map((pair) => pair.trim())
    .filter(Boolean)
    .map((pair) => {
      const index = pair.indexOf('=');
      if (index < 1) throw new Error(`"${pair}" must use key=value syntax`);
      return [pair.slice(0, index).trim(), pair.slice(index + 1).trim()] as const;
    });
  return Object.fromEntries(entries);
}

export function CreateMaterializationDialog({
  open,
  onClose,
  materializations = NO_MATERIALIZATIONS,
}: {
  open: boolean;
  onClose: () => void;
  /** The fleet, so topics that are already materialized can say so. */
  materializations?: readonly MaterializationSummary[];
}): ReactNode {
  const domainPath = useDomainPath();
  const api = useApi();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [submitted, setSubmitted] = useState(false);
  const [fieldError, setFieldError] = useState<{ field: string; message: string } | null>(null);
  // What the control plane said about a row - a name taken meanwhile - until the row changes.
  const [rowErrors, setRowErrors] = useState<ReadonlyMap<string, DraftProblem>>(new Map());
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const { defaults, datasetRules, tableRules, labelMaxLength, maxTopics, autoscaling: autoscalingPolicy } = useMaterializationDefaults();
  const datasets = useDatasets(open);
  const refreshDatasets = useRefreshDatasets();
  const navigate = useNavigate();
  const toast = useToast();

  const update = <K extends keyof FormState>(key: K, value: FormState[K]): void => {
    setForm((current) => ({ ...current, [key]: value }));
  };

  // A new topic's table goes in the dataset last chosen, named after the topic until renamed.
  const chooseTopics = (topics: string[]): void => {
    setForm((current) => ({
      ...current,
      source_topics: topics,
      tables: draftTables(topics, current.tables, {
        rules: tableRules,
        dataset: defaultDataset(current.tables, datasets.data?.datasets ?? null),
      }),
    }));
    if (fieldError?.field === 'tables') setFieldError(null);
  };

  const changeTable = (draft: TableDraft): void => {
    setForm((current) => {
      const tables = current.tables.map((existing) => (existing.topic === draft.topic ? draft : existing));
      return { ...current, tables: draftTables(current.source_topics, tables, { rules: tableRules, dataset: '' }) };
    });
    setRowErrors((current) => {
      if (!current.has(draft.topic)) return current;
      const next = new Map(current);
      next.delete(draft.topic);
      return next;
    });
  };

  // Surface the offending input even when it lives in the collapsed section.
  const reportFieldError = (field: string | null, message: string): void => {
    setFieldError(field === null ? null : { field, message });
    if (field !== null && ADVANCED_FIELDS.has(field)) setAdvancedOpen(true);
  };

  const nameProblem = labelProblem(form.name, labelMaxLength);
  const problems = useMemo(
    () => draftProblems(form.tables, { rules: tableRules, datasets: datasets.data?.datasets ?? null }),
    [form.tables, tableRules, datasets.data],
  );
  const problemOf = (draft: TableDraft, position: number): DraftProblem | null => rowErrors.get(draft.topic) ?? problems[position] ?? null;
  const tablesBlocked = form.tables.some((draft, position) => blocksSubmit(problemOf(draft, position)));
  // Every materialization's tables go under the deployment's destination: none, and none can be made.
  const destinationMissing = defaults !== null && !defaults.destinationUri;

  // Data protection: the domain's default, unless chosen otherwise - which needs Choose data
  // protection on every dataset a table goes in (the control plane decides; this says so first).
  const catalog = defaults?.protection ?? null;
  const defaultChoice = useMemo(() => choiceOf(catalog?.default ?? {}, catalog), [catalog]);
  const protection = form.protection ?? defaultChoice;
  const protectionLockedBy = useMemo(() => {
    const byName = new Map((datasets.data?.datasets ?? []).map((dataset) => [dataset.name, dataset]));
    const names = [...new Set(form.tables.map((draft) => draft.dataset).filter(Boolean))];
    return names.filter((name) => {
      const dataset = byName.get(name);
      return dataset !== undefined && !dataset.permissions.includes('dataset.protect');
    });
  }, [form.tables, datasets.data]);
  const protectionChanged = !sameChoice(protection, defaultChoice);

  const mutation = useMutation({
    mutationFn: (input: CreateMaterializationInput) => api.create(input),
    onSuccess: (accepted) => {
      toast.success(
        'Materialization accepted',
        accepted.claim_status === 'pending'
          ? `${accepted.materialization_id}: its tables’ names are still being claimed; then it is discovered and deployed.`
          : `${accepted.materialization_id} is being discovered and deployed.`,
      );
      setForm(EMPTY);
      setSubmitted(false);
      setRowErrors(new Map());
      setAdvancedOpen(false);
      void refreshDatasets();
      onClose();
      navigate(domainPath(`/materializations/${accepted.materialization_id}`));
    },
    onError: (error: unknown, input) => {
      const message = error instanceof ApiError ? error.message : 'The control plane rejected the request.';
      const row = error instanceof ApiError ? claimRow(error.details) : null;
      const topic = row ? input.tables[row.index]?.source_topic : undefined;
      if (row && topic) {
        // On the row it is about: a name another request took first, a dataset gone, access taken
        // away, a topic Kafka does not list.
        setRowErrors(new Map([[topic, rowProblem(row.field, message)]]));
        setFieldError(null);
        void refreshDatasets();
      } else {
        const field = error instanceof ApiError ? ((error.details.field as string | undefined) ?? null) : null;
        reportFieldError(field, message);
      }
      toast.error('Could not create materialization', message);
    },
  });

  const onSubmit = (event: FormEvent): void => {
    event.preventDefault();
    setSubmitted(true);
    setFieldError(null);
    if (nameProblem || tablesBlocked || destinationMissing) return;
    let labels: Record<string, string>;
    try {
      labels = parseLabels(form.labels);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Use key=value pairs.';
      reportFieldError('labels', 'Use key=value pairs');
      toast.error('Invalid labels', message);
      return;
    }

    const replicas = form.replicas.trim();
    const maxReplicas = defaults?.maxReplicas;
    if (replicas && (!/^\d+$/.test(replicas) || Number(replicas) < 1 || (maxReplicas && Number(replicas) > maxReplicas))) {
      const message = maxReplicas ? `A whole number from 1 to ${maxReplicas}` : 'A whole number of 1 or more';
      reportFieldError('replicas', message);
      toast.error('Invalid replicas', message);
      return;
    }

    mutation.mutate({
      ...(form.name.trim() ? { name: form.name.trim() } : {}),
      tables: tableRequests(form.tables),
      ...(replicas ? { replicas: Number(replicas) } : {}),
      // On is the control plane's own default, and a control plane before 4.4 knows no such field.
      ...(form.autoscaling ? {} : { autoscaling: false }),
      ...(Object.keys(labels).length > 0 ? { labels } : {}),
      ...(form.skip_tombstones ? { skip_tombstones: true } : {}),
      ...(form.partition_granularity ? { partition_granularity: form.partition_granularity } : {}),
      // Sent only when it differs from the default: matching it is no decision at all.
      ...(catalog && protectionChanged ? { protection: { kinds: protection } } : {}),
    });
  };

  const ready =
    form.source_topics.length > 0 &&
    form.source_topics.length <= maxTopics &&
    !nameProblem &&
    !destinationMissing &&
    !tablesBlocked &&
    !(protectionChanged && protectionLockedBy.length > 0);
  const errorFor = (field: string): string | null => (fieldError?.field === field ? fieldError.message : null);
  const first = form.tables[0];

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="New materialization"
      description="Choose one or more topics, and the dataset each one’s table goes in. The control plane infers each one’s schema, creates an Iceberg table for each, then deploys one runtime that reads them all."
      footer={
        <>
          <Button variant="ghost" onClick={onClose} type="button">
            Cancel
          </Button>
          <Button
            variant="primary"
            type="submit"
            form="create-materialization"
            disabled={!ready}
            loading={mutation.isPending}
          >
            Create
          </Button>
        </>
      }
    >
      <form id="create-materialization" onSubmit={onSubmit} className="space-y-4">
        {destinationMissing ? (
          <p
            role="alert"
            className="rounded-[var(--radius-control)] border border-[color-mix(in_srgb,var(--color-phase-failed)_35%,transparent)] bg-[color-mix(in_srgb,var(--color-phase-failed)_8%,transparent)] px-3 py-2.5 text-[12px] text-[var(--color-ink)]"
          >
            No materialization can be created yet: this control plane has no destination for tables. An administrator
            sets one on the Settings page, under Materializations › Destination.
          </p>
        ) : null}
        <TopicPicker
          label="Source topics"
          required
          value={form.source_topics}
          onChange={chooseTopics}
          maxTopics={maxTopics}
          materializations={materializations}
          renderTable={(topic, position) => {
            const draft = form.tables.find((candidate) => candidate.topic === topic);
            if (!draft) return null;
            const problem = problemOf(draft, position);
            return (
              <TableDraftRow
                draft={draft}
                problem={problem}
                // "Choose a dataset" waits for a submit; anything else is said at once.
                showProblem={submitted || rowErrors.has(topic) || (problem !== null && (problem.field === 'name' || draft.dataset !== ''))}
                datasetRules={datasetRules}
                onChange={changeTable}
              />
            );
          }}
          error={
            errorFor('tables') ??
            errorFor('source_topics') ??
            (submitted && form.source_topics.length === 0 ? 'Choose at least one topic' : null)
          }
          autoFocus
        />
        {first ? (
          <p className="-mt-2 text-[12px] text-[var(--color-ink-muted)]">
            SQL reads each table as <code className="numeric">{first.dataset || 'dataset'}.{first.name || 'table'}</code>, and
            engines on the catalog as <code className="numeric">lake.{first.dataset || 'dataset'}.{first.name || 'table'}</code>.
            A table’s dataset and name are fixed once it is created.
          </p>
        ) : null}

        <Field
          label="Name"
          hint="Optional: a label for people, which can be changed at any time. Left empty, the materialization goes by its id."
          error={errorFor('name') ?? nameProblem}
        >
          <Input
            value={form.name}
            onChange={(event) => {
              update('name', event.target.value);
              if (fieldError?.field === 'name') setFieldError(null);
            }}
            placeholder="Orders and invoices"
            autoComplete="off"
            maxLength={labelMaxLength + 16}
            aria-invalid={nameProblem !== null || errorFor('name') !== null}
          />
        </Field>

        {catalog ? (
          <ProtectionField
            idPrefix="create-protection"
            catalog={catalog}
            value={protection}
            defaultChoice={defaultChoice}
            lockedBy={protectionLockedBy}
            error={errorFor('protection')}
            onChange={(choice) => {
              update('protection', choice);
              if (fieldError?.field?.startsWith('protection')) setFieldError(null);
            }}
            note="Applies to every record this materialization writes. Rows are masked before they are written, so no query ever sees them."
          />
        ) : null}

        <div className="rounded-[var(--radius-control)] border border-[var(--color-line)]">
          <button
            type="button"
            onClick={() => setAdvancedOpen((current) => !current)}
            aria-expanded={advancedOpen}
            aria-controls="create-materialization-advanced"
            className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-[13px] font-medium text-[var(--color-ink)] transition-colors hover:bg-[var(--color-raised)]"
          >
            <ChevronRight
              className={cn('size-3.5 shrink-0 transition-transform', advancedOpen && 'rotate-90')}
            />
            <span className="shrink-0">Advanced settings</span>
            <span className="ml-auto min-w-0 truncate text-[11px] font-normal text-[var(--color-ink-faint)]">
              Partitioning, replicas, autoscaling, labels, tombstones
            </span>
          </button>

          {advancedOpen ? (
            <div
              id="create-materialization-advanced"
              className="space-y-4 border-t border-[var(--color-line)] px-3 py-4"
            >
              {defaults?.destinationUri ? (
                <p className="text-[12px] text-[var(--color-ink-muted)]">
                  Its tables are created under the deployment’s destination,{' '}
                  <code className="numeric break-words">{defaults.destinationUri}</code>, and each runtime member gets{' '}
                  <code className="numeric">{defaults.storageSize}</code> of storage.
                </p>
              ) : null}

              <Field
                label="Partitioning"
                hint={
                  <>
                    How the table’s files are split by each record’s Kafka timestamp, so a time range reads only
                    its partitions. Day suits most topics; hour suits topics writing many gigabytes an hour, and
                    month topics writing a few megabytes a day.
                    Defaults to {GRANULARITY_LABELS[defaults?.partitionGranularity ?? 'day'].toLowerCase()}; fixed
                    once created.
                  </>
                }
                error={errorFor('partition_granularity')}
              >
                <Select
                  value={form.partition_granularity}
                  onChange={(event) => update('partition_granularity', event.target.value as PartitionGranularity | '')}
                >
                  <option value="">Default ({GRANULARITY_LABELS[defaults?.partitionGranularity ?? 'day'].toLowerCase()})</option>
                  {(Object.keys(GRANULARITY_LABELS) as PartitionGranularity[]).map((value) => (
                    <option key={value} value={value}>
                      {GRANULARITY_LABELS[value]}
                    </option>
                  ))}
                </Select>
              </Field>

              <Field
                label="Replicas"
                hint={`Runtime members, which share every topic's partitions. Defaults to ${defaults?.replicas ?? FALLBACK_REPLICAS}; can be changed later.`}
                error={errorFor('replicas')}
              >
                <Input
                  value={form.replicas}
                  onChange={(event) => update('replicas', event.target.value)}
                  placeholder={String(defaults?.replicas ?? FALLBACK_REPLICAS)}
                  inputMode="numeric"
                  spellCheck={false}
                />
              </Field>

              {autoscalingPolicy ? (
                <label className="flex items-start gap-2.5">
                  <input
                    type="checkbox"
                    checked={form.autoscaling}
                    onChange={(event) => update('autoscaling', event.target.checked)}
                    className="mt-[3px] size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
                  />
                  <span className="space-y-1.5">
                    <span className="block text-[13px] font-medium">Autoscale</span>
                    <span className="block text-[12px] text-[var(--color-ink-muted)]">
                      {autoscalingPolicy.enabled
                        ? `Adds members while it is far behind its topics, up to ${autoscalingPolicy.maxReplicas}, and goes back to its replicas once caught up. Can be changed later.`
                        : 'Autoscaling is off for every materialization (Settings › Materializations). Left on, this one autoscales once it is turned on there.'}
                    </span>
                  </span>
                </label>
              ) : null}

              <Field
                label="Labels"
                hint="Comma separated key=value pairs."
                error={errorFor('labels')}
              >
                <Input
                  value={form.labels}
                  onChange={(event) => update('labels', event.target.value)}
                  placeholder="team=data-platform, tier=gold"
                  spellCheck={false}
                />
              </Field>

              <label className="flex items-start gap-2.5">
                <input
                  type="checkbox"
                  checked={form.skip_tombstones}
                  onChange={(event) => update('skip_tombstones', event.target.checked)}
                  className="mt-[3px] size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
                />
                <span className="space-y-1.5">
                  <span className="block text-[13px] font-medium">Skip tombstones</span>
                  <span className="block text-[12px] text-[var(--color-ink-muted)]">
                    Ignore null-value records (deletes on compacted topics). When off, each one becomes a
                    row with its Kafka key and an empty payload.
                  </span>
                </span>
              </label>
            </div>
          ) : null}
        </div>
      </form>
    </Dialog>
  );
}
