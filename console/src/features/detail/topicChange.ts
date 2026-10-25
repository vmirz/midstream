// Type-only, so `node --test` can load this module without the bundler's path aliases.
import type { CpDetail, CpTableSpec } from '@shared/types';

/** What choosing `topics` does to the materialization's tables, as the control plane plans it. */
export interface TopicChangeDraft {
  /** The table each topic keeps: the written tables' and the kept ones'. */
  fixed: ReadonlyMap<string, string>;
  /** Names no new table may take. */
  taken: string[];
  /** Written tables whose topics are no longer chosen: each needs a decision. */
  removed: CpTableSpec[];
  /** Kept tables whose topics are chosen again. */
  resumed: CpTableSpec[];
  /** Kept tables whose topics stay removed: each may be dropped. */
  stillKept: CpTableSpec[];
  /** Topics with no table yet. */
  added: string[];
  changesTopics: boolean;
  notes: ReadonlyMap<string, string>;
}

export function planTopicChange(detail: CpDetail, topics: readonly string[]): TopicChangeDraft {
  const { materialization } = detail;
  const kept = materialization.retired_tables ?? [];
  const chosen = new Set(topics);
  const fixed = new Map([...materialization.tables, ...kept].map((spec) => [spec.source_topic, spec.name]));
  const taken = detail.tables.filter((record) => record.state === 'dropping' || record.state === 'dropped').map((record) => record.name);
  const removed = materialization.tables.filter((spec) => !chosen.has(spec.source_topic));
  const resumed = kept.filter((spec) => chosen.has(spec.source_topic));
  const stillKept = kept.filter((spec) => !chosen.has(spec.source_topic));
  const added = topics.filter((topic) => !fixed.has(topic));
  const notes = new Map<string, string>([
    ...added.map((topic) => [topic, 'new table'] as [string, string]),
    ...resumed.map((spec) => [spec.source_topic, 'kept table, written again'] as [string, string]),
  ]);
  return {
    fixed,
    taken,
    removed,
    resumed,
    stillKept,
    added,
    changesTopics: removed.length > 0 || resumed.length > 0 || added.length > 0,
    notes,
  };
}

/**
 * `next` in the order the materialization already reads its topics, any other after them as chosen:
 * the order of the topics orders the tables, and changing it alone would restart the runtime for nothing.
 */
export function keepOrder(existing: readonly string[], next: readonly string[]): string[] {
  const chosen = new Set(next);
  const known = new Set(existing);
  return [...existing.filter((topic) => chosen.has(topic)), ...next.filter((topic) => !known.has(topic))];
}
