import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Loader2, ShieldCheck } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { RelativeTime } from '@/components/Inline';
import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, Panel, PanelHeader } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { useApi } from '@/lib/domains';
import { formatBytes, formatExact } from '@/lib/format';
import {
  readableTables,
  type CpDetail,
  type CpMaintenanceRun,
  type CpPlaintextReport,
  type CpProtectionReview,
  type CpReprotectRequest,
} from '@shared/types';

import { plaintextTotals, reprotectCounts } from './protection';

/** One table a reprotect rewrites: written or kept. */
interface ReprotectRow {
  table: string;
  dataset: string;
  kept: boolean;
  plaintext: CpPlaintextReport | null;
  asked: CpReprotectRequest | null;
  last: CpMaintenanceRun | null;
  running: boolean;
}

function rowsOf(review: CpProtectionReview, detail: CpDetail | null): ReprotectRow[] {
  const running = (table: string): boolean => detail?.tables.find((record) => record.name === table)?.maintenance_job?.job === 'reprotect';
  return [
    ...review.review
      .filter((table) => table.created)
      .map((table) => ({
        table: table.table,
        dataset: table.dataset,
        kept: false,
        plaintext: table.plaintext ?? null,
        asked: table.reprotect ?? null,
        last: table.last_reprotect ?? null,
        running: running(table.table),
      })),
    ...(review.kept ?? []).map((table) => ({
      table: table.table,
      dataset: table.dataset,
      kept: true,
      plaintext: table.plaintext,
      asked: table.reprotect,
      last: table.last_reprotect,
      running: running(table.table),
    })),
  ];
}

/**
 * Rows already written (handbook 22 §22.15): what each table holds in plain text that it masks now,
 * as the maintenance app last counted it, and the action that masks it - a reprotect of every table,
 * with or without a purge of the older snapshots.
 */
export function ReprotectPanel({ review, detail }: { review: CpProtectionReview; detail: CpDetail | null }): ReactNode {
  const { can } = useAuth();
  const [confirming, setConfirming] = useState(false);
  const rows = rowsOf(review, detail);
  if (rows.length === 0 || review.kept === undefined) return null;
  const totals = plaintextTotals(rows.map((row) => row.plaintext));
  const mayReprotect =
    can('materializations.maintain') && readableTables(detail?.materialization ?? { tables: [] }).every((spec) => can('dataset.protect', spec.dataset_id));
  const busy = rows.some((row) => row.running || row.asked !== null);
  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title="Rows already written"
        description={summary(totals, rows.length)}
        actions={
          mayReprotect ? (
            <Button size="sm" variant={totals.files > 0 ? 'primary' : 'secondary'} onClick={() => setConfirming(true)}>
              <ShieldCheck className="size-3.5" />
              Mask rows already written
            </Button>
          ) : null
        }
      />
      <ul className="divide-y divide-[var(--color-line)]">
        {rows.map((row) => (
          <ReprotectRowItem key={`${row.dataset}.${row.table}`} row={row} />
        ))}
      </ul>
      <p className="border-t border-[var(--color-line)] px-5 py-3 text-[12px] text-[var(--color-ink-muted)]">
        Counted in each table’s current snapshot. Older snapshots keep the files a reprotect replaced, and time travel still reads them,
        until they expire (Settings › Compaction &amp; retention) — unless the reprotect purges them.
        {busy ? ' A reprotect asked for runs at each table’s next opportunity, one job at a time.' : ''}
      </p>
      <ReprotectDialog open={confirming} onClose={() => setConfirming(false)} materializationId={review.materialization_id} rows={rows} />
    </Panel>
  );
}

function summary(totals: ReturnType<typeof plaintextTotals>, tables: number): string {
  if (totals.counted === 0) {
    return 'Not counted yet: the maintenance app counts each table’s files when it records a new revision, and as it compacts.';
  }
  const parts: string[] = [];
  if (totals.files > 0) {
    parts.push(
      `${formatExact(totals.files)} ${totals.files === 1 ? 'file' : 'files'} with ${formatExact(totals.rows)} ${totals.rows === 1 ? 'row' : 'rows'} hold in plain text values ${tables === 1 ? 'the table masks' : 'these tables mask'} now: they were written before the field was masked.`,
    );
  } else {
    parts.push(`No file holds in plain text a value ${tables === 1 ? 'the table masks' : 'these tables mask'} now.`);
  }
  if (totals.blockedFiles > 0) parts.push(`${formatExact(totals.blockedFiles)} more cannot be rewritten yet.`);
  return parts.join(' ');
}

function ReprotectRowItem({ row }: { row: ReprotectRow }): ReactNode {
  const report = row.plaintext;
  const last = row.last;
  const counts = last ? reprotectCounts(last) : [];
  return (
    <li className="px-5 py-3 text-[13px]">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="flex min-w-0 items-center gap-2">
          <code className="numeric truncate">
            {row.dataset}.{row.table}
          </code>
          {row.kept ? <Badge>kept</Badge> : null}
        </span>
        <span className="flex flex-wrap items-center gap-2 text-[12px] text-[var(--color-ink-muted)]">
          {report === null ? (
            'not counted yet'
          ) : report.files > 0 ? (
            <span className="numeric text-[var(--color-ink)]">
              {formatExact(report.files)} {report.files === 1 ? 'file' : 'files'} · {formatExact(report.rows)} {report.rows === 1 ? 'row' : 'rows'} ·{' '}
              {formatBytes(report.bytes)}
            </span>
          ) : (
            'none in plain text'
          )}
          {row.running ? (
            <Badge tone="pending">
              <Loader2 className="size-3 animate-spin" />
              rewriting
            </Badge>
          ) : row.asked ? (
            <Badge tone="pending" title={`Asked by ${row.asked.requested_by}${row.asked.purge ? ', with a purge' : ''}`}>
              requested
            </Badge>
          ) : last ? (
            <LastRunBadge outcome={last.outcome} />
          ) : null}
        </span>
      </div>
      {report && report.blocked_files > 0 ? (
        <p className="mt-1 text-[12px] text-[var(--color-phase-paused)]">
          {formatExact(report.blocked_files)} {report.blocked_files === 1 ? 'file' : 'files'} cannot be rewritten yet: {report.blocked.join('; ')}.
        </p>
      ) : null}
      {report && report.unknown_files > 0 ? (
        <p className="mt-1 text-[12px] text-[var(--color-ink-muted)]">
          {formatExact(report.unknown_files)} {report.unknown_files === 1 ? 'file was' : 'files were'} written under revisions the table no longer
          describes, and {report.unknown_files === 1 ? 'is' : 'are'} left as written.
        </p>
      ) : null}
      {last ? (
        <p className="numeric mt-1 text-[12px] text-[var(--color-ink-muted)]">
          Last reprotect <RelativeTime value={last.completed_at} />
          {counts.length > 0 ? ` · ${counts.join(' · ')}` : ''}
        </p>
      ) : null}
      {last?.message && last.outcome !== 'succeeded' ? (
        <p className={cn('mt-1 text-[12px]', last.outcome === 'failed' ? 'text-[var(--color-phase-failed)]' : 'text-[var(--color-ink-muted)]')}>
          {last.message}
        </p>
      ) : null}
    </li>
  );
}

function LastRunBadge({ outcome }: { outcome: string }): ReactNode {
  if (outcome === 'succeeded') return <Badge tone="running">masked</Badge>;
  if (outcome === 'skipped') return <Badge>nothing to mask</Badge>;
  if (outcome === 'failed') return <Badge tone="failed">failed</Badge>;
  return <Badge tone="pending">{outcome}</Badge>;
}

function ReprotectDialog({
  open,
  onClose,
  materializationId,
  rows,
}: {
  open: boolean;
  onClose: () => void;
  materializationId: string;
  rows: ReprotectRow[];
}): ReactNode {
  const api = useApi();
  const toast = useToast();
  const queryClient = useQueryClient();
  const [purge, setPurge] = useState(true);
  const reprotect = useMutation({
    mutationFn: () => api.maintain(materializationId, 'reprotect', null, { purge }),
    onSuccess: () => {
      toast.success(
        'Masking the rows already written',
        'The maintenance app rewrites each table’s files at its next opportunity, one job at a time, and reports here.',
      );
      void queryClient.invalidateQueries({ queryKey: ['protection', materializationId] });
      onClose();
    },
    onError: (error: unknown) => toast.error('Could not ask for a reprotect', error instanceof ApiError ? error.message : String(error)),
  });
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Mask rows already written?"
      description="The maintenance app rewrites each file holding in plain text a value its table masks now, with the mask the table records for that field. Values masked before stay as they were; the rows, their order and the offsets each table records are unchanged."
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" loading={reprotect.isPending} onClick={() => reprotect.mutate()}>
            Mask rows
          </Button>
        </>
      }
    >
      <div className="space-y-4 text-[13px] leading-relaxed">
        <ul className="space-y-0.5">
          {rows.map((row) => (
            <li key={`${row.dataset}.${row.table}`} className="flex items-center justify-between gap-3">
              <code className="numeric">
                {row.dataset}.{row.table}
              </code>
              <span className="numeric text-[12px] text-[var(--color-ink-muted)]">
                {row.plaintext === null
                  ? 'not counted yet'
                  : `${formatExact(row.plaintext.files)} ${row.plaintext.files === 1 ? 'file' : 'files'} · ${formatExact(row.plaintext.rows)} ${row.plaintext.rows === 1 ? 'row' : 'rows'}`}
              </span>
            </li>
          ))}
        </ul>
        <label className="flex items-start gap-2.5">
          <input
            type="checkbox"
            checked={purge}
            onChange={(event) => setPurge(event.target.checked)}
            className="mt-[3px] size-3.5 shrink-0 cursor-pointer accent-[var(--color-accent)]"
          />
          <span className="space-y-1.5">
            <span className="block text-[13px] font-medium">Purge the plain text now</span>
            <span className="block text-[12px] text-[var(--color-ink-muted)]">
              Once every file is rewritten, expire each table’s older snapshots and delete the files they alone held: time travel to before
              the reprotect ends. Without it, the old files stay readable through older snapshots until they expire.
            </span>
          </span>
        </label>
      </div>
    </Dialog>
  );
}
