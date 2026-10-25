import { ArrowLeft } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { readJsonShape, writeJsonShape } from './jsonShape';
import { SegmentedControl } from '@/components/ui/primitives';
import { api } from '@/lib/api';
import { formatExact } from '@/lib/format';
import {
  RUN_EXPORT_FORMATS,
  type ConsoleConfig,
  type RunExportFormat,
  type RunExportShape,
  type RunSnapshot,
} from '@shared/types';

const FORMAT_LABEL: Record<RunExportFormat, string> = {
  csv: 'CSV',
  json: 'JSON',
  jsonl: 'JSON Lines',
  parquet: 'Parquet',
};

const FORMAT_HINT: Record<RunExportFormat, string> = {
  csv: 'Spreadsheets',
  json: 'One array',
  jsonl: 'One object per line',
  parquet: 'Columnar, smallest',
};

const SHAPE_EXAMPLE: Record<RunExportShape, string> = {
  nested: '{ "customer": { "email": … } }',
  flat: '{ "customer.email": … }',
};

/** How many rows an export of the run downloads: what it kept, up to the console's export limit. */
export function exportRows(run: RunSnapshot, config: ConsoleConfig | null): number {
  const available = run.request.kind === 'sql' ? (run.rowsRetained ?? 0) : run.rows;
  return Math.min(available, config?.exportRowLimit ?? Number.POSITIVE_INFINITY);
}

/**
 * Export the finished run's result: every row it matched, in the order shown. The Data tab's
 * More actions menu opens it.
 *
 * Offered only once a run has succeeded, because only then is there a complete answer. The rows
 * come from the run itself — never a re-execution and never the page on screen — so the file is
 * exactly what was looked at. Text formats stream, so the download starts at once.
 *
 * JSON comes in two layouts: nested, each row rebuilt into the record it came from, or flat, one
 * key per column as the grid shows it. The choice is remembered per browser.
 */
export function ExportOptions({
  run,
  config,
  onBack,
  onDone,
}: {
  run: RunSnapshot;
  config: ConsoleConfig | null;
  /** Back to the menu that opened it. */
  onBack: () => void;
  /** A download started. */
  onDone: () => void;
}): ReactNode {
  const [shape, setShape] = useState<RunExportShape>(readJsonShape);
  const limit = config?.exportRowLimit ?? Number.POSITIVE_INFINITY;
  const available = run.request.kind === 'sql' ? (run.rowsRetained ?? 0) : run.rows;
  const rows = exportRows(run, config);
  const capped = available > limit || run.truncated;

  const chooseShape = (next: RunExportShape): void => {
    setShape(next);
    writeJsonShape(next);
  };

  return (
    <div className="p-1.5" data-role="export-options">
      <button
        type="button"
        onClick={onBack}
        className="flex w-full items-center gap-1.5 rounded px-2 py-1 text-left text-[12px] font-medium text-[var(--color-ink)] transition-colors hover:bg-[var(--color-raised)]"
      >
        <ArrowLeft className="size-3.5 text-[var(--color-ink-muted)]" />
        Export
      </button>
      <p className="px-2 pt-1 pb-1.5 text-[11px] text-[var(--color-ink-muted)]">
        {capped
          ? `Downloads the first ${formatExact(rows)} of ${formatExact(run.rows)} rows.`
          : `Downloads all ${formatExact(rows)} rows of this result.`}
      </p>

      <div className="mx-2 mb-1.5 rounded-[var(--radius-control)] border border-[var(--color-line)] px-2 py-1.5">
        <div className="flex items-center justify-between gap-2">
          <span className="text-[11px] font-medium text-[var(--color-ink-muted)]">JSON layout</span>
          <SegmentedControl
            value={shape}
            onChange={chooseShape}
            options={[
              { value: 'nested', label: 'Nested' },
              { value: 'flat', label: 'Flat' },
            ]}
          />
        </div>
        <p className="numeric mt-1 truncate text-[10px] text-[var(--color-ink-faint)]" title={SHAPE_EXAMPLE[shape]}>
          {SHAPE_EXAMPLE[shape]}
        </p>
      </div>

      {RUN_EXPORT_FORMATS.map((format) => {
        const shaped = format === 'json' || format === 'jsonl';
        return (
          <a
            key={format}
            href={api.runExportUrl(run.id, format, shape)}
            download
            onClick={onDone}
            className="flex w-full items-baseline justify-between gap-2 rounded px-2 py-1.5 text-left transition-colors hover:bg-[var(--color-raised)]"
          >
            <span className="text-[12px] text-[var(--color-ink)]">{FORMAT_LABEL[format]}</span>
            <span className="text-[10px] text-[var(--color-ink-faint)]">
              {FORMAT_HINT[format]}
              {shaped ? ` · ${shape}` : ''}
            </span>
          </a>
        );
      })}
    </div>
  );
}
