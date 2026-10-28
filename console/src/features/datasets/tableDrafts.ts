/**
 * Each chosen topic's table while a dialog is open: the dataset it goes in, and its name.
 *
 * A topic picked gets the dataset last chosen and a name derived from the topic; the name follows
 * the topic until someone types one. The rows are checked as they change, against what the console
 * knows: the name rules, names unique in the materialization, and the names each dataset already
 * holds. The control plane checks again when the names are claimed; a refusal it sends names the row
 * (`tables.<index>.<field>`), and `claimRow` finds it. It also refuses a topic Kafka does not list, on
 * the topic's row: a problem with the topic itself, which no change to the row can fix.
 */

// Relative and type-only where it can be, so `node --test` loads this without the bundler's aliases.
import { qualifiedTableName, tableNameProblem, tableNamesForTopics, type NameRules } from '../../../shared/tableNames.ts';
import type { CpDataset, TableRequest } from '@shared/types';

/** A topic's table in the making. */
export interface TableDraft {
  topic: string;
  /** The dataset's name; empty until one is chosen. */
  dataset: string;
  name: string;
  /** Whether the name was typed; until then it follows the topic. */
  edited: boolean;
}

/** What is wrong with a row, and which of its fields; `topic` for the topic itself. */
export interface DraftProblem {
  field: 'dataset' | 'name' | 'topic';
  message: string;
}

/**
 * The drafts for `topics`, in their order. A topic already drafted keeps its draft; a new one gets
 * `dataset` and a derived name. Names nobody typed are derived again, so they stay unique among the
 * rows and clear of `taken` - the names the materialization's other tables have.
 */
export function draftTables(
  topics: readonly string[],
  previous: readonly TableDraft[],
  { rules, dataset, taken = [] }: { rules: NameRules; dataset: string; taken?: readonly string[] },
): TableDraft[] {
  const byTopic = new Map(previous.map((draft) => [draft.topic, draft]));
  const edited = new Map(previous.filter((draft) => draft.edited).map((draft) => [draft.topic, draft.name]));
  const names = tableNamesForTopics(topics, rules, { fixed: edited, taken });
  return topics.map((topic, index) => {
    const draft = byTopic.get(topic);
    const name = names[index] as string;
    if (draft) return draft.edited ? draft : { ...draft, name };
    return { topic, dataset, name, edited: false };
  });
}

/**
 * Each row's first problem, or null: no dataset, one the person cannot add tables to, a name SQL
 * cannot read, a name another row or table of the materialization has, or one the dataset already
 * holds. `datasets` is what the person was last told; the claim settles it.
 */
export function draftProblems(
  drafts: readonly TableDraft[],
  { rules, datasets, taken = [] }: { rules: NameRules; datasets: readonly CpDataset[] | null; taken?: readonly string[] },
): Array<DraftProblem | null> {
  const byName = new Map((datasets ?? []).map((dataset) => [dataset.name, dataset]));
  const counts = new Map<string, number>();
  for (const name of [...drafts.map((draft) => draft.name), ...taken]) {
    const key = name.trim().toLowerCase();
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return drafts.map((draft) => {
    if (!draft.dataset) return { field: 'dataset', message: 'Choose the dataset its table goes in' };
    const dataset = byName.get(draft.dataset);
    if (datasets !== null && !dataset) return { field: 'dataset', message: `There is no dataset ${draft.dataset}` };
    if (dataset && !dataset.permissions.includes('dataset.write')) {
      return { field: 'dataset', message: `You cannot add tables to ${dataset.name}` };
    }
    const problem = tableNameProblem(draft.name, rules);
    if (problem) return { field: 'name', message: problem };
    const name = draft.name.trim();
    if ((counts.get(name.toLowerCase()) ?? 0) > 1) return { field: 'name', message: `Another table of this materialization is named ${name}` };
    if (dataset?.tables.includes(name)) {
      return { field: 'name', message: `${qualifiedTableName(dataset.name, name)} is taken; choose another name` };
    }
    return null;
  });
}

/** The request's `tables` for these drafts, each with the name the dialog shows, derived or typed. */
export function tableRequests(drafts: readonly TableDraft[]): TableRequest[] {
  return drafts.map((draft) => ({ source_topic: draft.topic, dataset: draft.dataset, name: draft.name.trim() }));
}

/**
 * The row an error is about, and which of its fields: the control plane names it `tables.<index>.<field>`,
 * or gives `index` in its details. Null for an error about the request as a whole.
 */
export function claimRow(details: Record<string, unknown>): { index: number; field: 'dataset' | 'name' | 'source_topic' | null } | null {
  const upstream = details.upstream_details;
  const sources = [details, upstream && typeof upstream === 'object' ? (upstream as Record<string, unknown>) : {}];
  for (const source of sources) {
    const field = typeof source.field === 'string' ? source.field : '';
    const match = /^tables\.(\d+)(?:\.(dataset|name|source_topic))?$/.exec(field);
    if (match) return { index: Number(match[1]), field: (match[2] as 'dataset' | 'name' | 'source_topic' | undefined) ?? null };
    if (typeof source.index === 'number' && Number.isInteger(source.index)) {
      const reason = source.reason;
      return { index: source.index, field: reason === 'dataset_not_found' || reason === 'forbidden' ? 'dataset' : 'name' };
    }
  }
  return null;
}

/** The problem a refusal names on a row: its dataset, the topic itself, or else its name. */
export function rowProblem(field: 'dataset' | 'name' | 'source_topic' | null, message: string): DraftProblem {
  return { field: field === 'dataset' ? 'dataset' : field === 'source_topic' ? 'topic' : 'name', message };
}

/**
 * Whether a row's problem keeps the form from being sent. One with the topic does not: Kafka not
 * listing it is a fact about the cluster, which can change - the topic created, or the deployment's
 * Kafka user allowed to see it - while the row stays as it is. The control plane asks again.
 */
export function blocksSubmit(problem: DraftProblem | null): boolean {
  return problem !== null && problem.field !== 'topic';
}

/** The dataset to start a new row with: the last one chosen, else the only one the person may add tables to. */
export function defaultDataset(drafts: readonly TableDraft[], datasets: readonly CpDataset[] | null): string {
  const last = [...drafts].reverse().find((draft) => draft.dataset)?.dataset;
  if (last) return last;
  const writable = (datasets ?? []).filter((dataset) => dataset.permissions.includes('dataset.write'));
  return writable.length === 1 ? (writable[0] as CpDataset).name : '';
}
