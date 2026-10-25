import { ArrowRight } from 'lucide-react';
import type { ReactNode } from 'react';

import { Input } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import type { NameRules } from '@shared/tableNames';

import { DatasetPicker } from './DatasetPicker';
import type { DraftProblem, TableDraft } from './tableDrafts';

/**
 * Where a chosen topic's rows go: the dataset, and the table's name in it - `sales . orders`, as SQL
 * reads it. The name starts as the one derived from the topic and can be changed until the table is
 * created; neither changes after.
 */
export function TableDraftRow({
  draft,
  problem,
  showProblem,
  datasetRules,
  onChange,
}: {
  draft: TableDraft;
  problem: DraftProblem | null;
  /** Whether to say what is wrong yet: once the row was touched or a submit was tried. */
  showProblem: boolean;
  datasetRules: NameRules;
  onChange: (draft: TableDraft) => void;
}): ReactNode {
  const shown = showProblem ? problem : null;
  return (
    <div className="space-y-1">
      <div className="flex items-center gap-1.5">
        <ArrowRight className="size-3 shrink-0 text-[var(--color-ink-faint)]" aria-hidden />
        <DatasetPicker
          value={draft.dataset}
          onChange={(dataset) => onChange({ ...draft, dataset })}
          rules={datasetRules}
          invalid={shown?.field === 'dataset'}
          label={`Dataset for ${draft.topic}`}
        />
        <span className="shrink-0 text-[var(--color-ink-faint)]" aria-hidden>
          .
        </span>
        <Input
          value={draft.name}
          onChange={(event) => onChange({ ...draft, name: event.target.value, edited: true })}
          aria-label={`Table name for ${draft.topic}`}
          aria-invalid={shown?.field === 'name'}
          spellCheck={false}
          autoComplete="off"
          className={cn('numeric h-8 min-w-0 flex-1 text-[13px]', shown?.field === 'name' && 'border-[var(--color-phase-failed)]')}
        />
      </div>
      {shown ? <p className="pl-[18px] text-[11px] text-[var(--color-phase-failed)]">{shown.message}</p> : null}
    </div>
  );
}

/** A table that already exists: its dataset and name are fixed. */
export function FixedTableRow({ dataset, name, note }: { dataset: string; name: string; note?: string }): ReactNode {
  return (
    <div className="flex items-center gap-1.5 text-[12px]">
      <ArrowRight className="size-3 shrink-0 text-[var(--color-ink-faint)]" aria-hidden />
      <code className="numeric min-w-0 truncate text-[var(--color-ink)]" title={`SELECT * FROM ${dataset}.${name}`}>
        {dataset}.{name}
      </code>
      {note ? <span className="shrink-0 text-[var(--color-ink-faint)]">· {note}</span> : null}
    </div>
  );
}
