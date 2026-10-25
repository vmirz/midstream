import { ChevronFirst, ChevronLast, ChevronLeft, ChevronRight } from 'lucide-react';
import { useEffect, useState, type ReactNode } from 'react';

import { PAGE_SIZES } from './viewState';
import { Button, Input, Select } from '@/components/ui/primitives';
import { formatExact } from '@/lib/format';
import type { RunSnapshot } from '@shared/types';

/**
 * Footer paging: the way to any page of the result. The run's exact row count is at the top of the
 * table (`StatusBar`), and the page box says where this page sits.
 *
 * Any page is one direct read, so the last page and page 40,000 are as reachable as the next one,
 * and the controls offer them. While a run is still counting, the total is a floor and only the
 * pages already counted can be reached directly; the rest arrive as counting reaches them.
 */
export function Pager({
  run,
  page,
  pageSize,
  onPage,
  onPageSize,
}: {
  run: RunSnapshot | null;
  page: number;
  pageSize: number;
  onPage: (page: number) => void;
  onPageSize: (size: number) => void;
}): ReactNode {
  const pageable = run ? (run.request.kind === 'sql' ? (run.rowsRetained ?? 0) : run.rowsReady) : 0;
  const settled = run !== null && (run.state !== 'running' || run.totalExact);
  const pages = settled ? Math.max(1, Math.ceil(pageable / pageSize)) : null;
  const reachable = Math.max(1, Math.ceil(pageable / pageSize));
  const canNext = pages !== null ? page + 1 < pages : run?.state === 'running';

  const [draft, setDraft] = useState(String(page + 1));
  useEffect(() => setDraft(String(page + 1)), [page]);
  const jump = (): void => {
    const target = Number.parseInt(draft, 10);
    if (!Number.isFinite(target)) return setDraft(String(page + 1));
    onPage(Math.min(Math.max(target, 1), pages ?? reachable) - 1);
  };

  return (
    <div className="flex min-h-[48px] flex-wrap items-center gap-x-3 gap-y-2 border-t border-[var(--color-line)] px-3 py-2">
      <span className="ml-auto flex items-center gap-1">
        <Select
          value={String(pageSize)}
          onChange={(event) => onPageSize(Number(event.target.value))}
          className="h-8 w-[108px] text-[12px]"
          aria-label="Rows per page"
        >
          {PAGE_SIZES.map((size) => (
            <option key={size} value={size}>
              {size} rows
            </option>
          ))}
        </Select>
        <Button size="sm" variant="ghost" className="px-1.5" disabled={page === 0} onClick={() => onPage(0)} aria-label="First page">
          <ChevronFirst className="size-4" />
        </Button>
        <Button size="sm" variant="ghost" className="px-1.5" disabled={page === 0} onClick={() => onPage(page - 1)} aria-label="Previous page">
          <ChevronLeft className="size-4" />
        </Button>
        <form
          className="flex items-center gap-1.5 text-[12px] text-[var(--color-ink-muted)]"
          onSubmit={(event) => {
            event.preventDefault();
            jump();
          }}
        >
          <span>Page</span>
          <Input
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onBlur={jump}
            inputMode="numeric"
            aria-label="Page number"
            className="numeric h-8 w-16 text-center text-[12px]"
          />
          <span className="numeric whitespace-nowrap">
            of {pages === null ? `${formatExact(reachable)}+` : formatExact(pages)}
          </span>
        </form>
        <Button size="sm" variant="ghost" className="px-1.5" disabled={!canNext} onClick={() => onPage(page + 1)} aria-label="Next page">
          <ChevronRight className="size-4" />
        </Button>
        <Button
          size="sm"
          variant="ghost"
          className="px-1.5"
          disabled={pages === null || page + 1 >= pages}
          onClick={() => pages !== null && onPage(pages - 1)}
          aria-label="Last page"
        >
          <ChevronLast className="size-4" />
        </Button>
      </span>
    </div>
  );
}
