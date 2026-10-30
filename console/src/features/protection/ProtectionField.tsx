import { AlertTriangle, ChevronRight, Lock, Shield } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { Button } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import type { CpProtectionCatalog } from '@shared/types';

import { ProtectionControl } from './ProtectionControl';
import { describeChoice, hashes, maskName, sameChoice, type ProtectionChoice } from './protection';

/**
 * Data protection in a dialog (handbook 22): what the materialization masks, in a line, and the control
 * to choose otherwise. Locked, with the reason, when the person may not choose protection on a dataset
 * the materialization writes into; warned when the choice hashes and the domain has no key.
 */
export function ProtectionField({
  idPrefix,
  catalog,
  value,
  defaultChoice,
  onChange,
  lockedBy,
  error,
  note,
  title = 'Data protection',
  startOpen = false,
  baselineTag = 'default',
  resetLabel = 'Use the default',
}: {
  idPrefix: string;
  catalog: CpProtectionCatalog;
  value: ProtectionChoice;
  /** What the choice starts from: the domain's default, which a create may keep without any
   * permission, or the policy an edit would replace. */
  defaultChoice: ProtectionChoice;
  onChange: (value: ProtectionChoice) => void;
  /** Datasets on which the person may not choose data protection; non-empty locks the control. */
  lockedBy: string[];
  error?: string | null;
  note?: ReactNode;
  title?: string;
  startOpen?: boolean;
  /** How the summary names an unchanged choice: `default`, or `current` in an edit. */
  baselineTag?: string;
  /** The button that returns to `defaultChoice`. */
  resetLabel?: string;
}): ReactNode {
  const [open, setOpen] = useState(startOpen);
  const locked = lockedBy.length > 0;
  const changed = !sameChoice(value, defaultChoice);
  const options = catalog.kinds.map((kind) => ({
    value: kind.id,
    label: kind.label,
    description: kind.description,
    choices: kind.masks,
    partialExample: kind.partial_example,
  }));
  const labels = Object.fromEntries(catalog.masks.map((mask) => [mask.id, mask.label]));
  const lockText =
    lockedBy.length === 1
      ? `Choosing other protection needs Choose data protection on dataset ${lockedBy[0]}.`
      : `Choosing other protection needs Choose data protection on datasets ${lockedBy.join(', ')}.`;
  return (
    <section aria-labelledby={`${idPrefix}-title`} className="rounded-[var(--radius-control)] border border-[var(--color-line)]">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-expanded={open}
        aria-controls={`${idPrefix}-body`}
        className="flex w-full items-center gap-2 px-3 py-2.5 text-left transition-colors hover:bg-[var(--color-raised)]"
      >
        <ChevronRight className={cn('size-3.5 shrink-0 transition-transform', open && 'rotate-90')} />
        <Shield className="size-3.5 shrink-0 text-[var(--color-accent)]" aria-hidden />
        <span id={`${idPrefix}-title`} className="shrink-0 text-[13px] font-medium">
          {title}
        </span>
        <span className="ml-auto min-w-0 truncate text-[11px] text-[var(--color-ink-muted)]" title={describeChoice(value, catalog)}>
          {describeChoice(value, catalog)}
          {changed ? '' : ` · ${baselineTag}`}
        </span>
      </button>
      {open ? (
        <div id={`${idPrefix}-body`} className="space-y-3 border-t border-[var(--color-line)] px-3 py-3">
          {note ? <p className="text-[12px] text-[var(--color-ink-muted)]">{note}</p> : null}
          {locked ? (
            <p className="flex items-start gap-1.5 text-[12px] text-[var(--color-ink-muted)]">
              <Lock className="mt-[2px] size-3.5 shrink-0" aria-hidden />
              <span>{lockText}</span>
            </p>
          ) : null}
          <ProtectionControl
            idPrefix={idPrefix}
            options={options}
            labels={labels}
            value={value}
            disabled={locked}
            onChange={(next) => onChange(next as ProtectionChoice)}
          />
          {changed && !locked ? (
            <Button size="sm" variant="ghost" type="button" onClick={() => onChange(defaultChoice)}>
              {resetLabel}
            </Button>
          ) : null}
        </div>
      ) : null}
      {hashes(value) && !catalog.key_configured ? (
        <p role="alert" className="flex items-start gap-1.5 border-t border-[var(--color-line)] px-3 py-2.5 text-[12px] text-[var(--color-phase-failed)]">
          <AlertTriangle className="mt-[2px] size-3.5 shrink-0" aria-hidden />
          <span>
            This domain has no protection key, so nothing can be {maskName('hash', catalog).toLowerCase()}: the control
            plane refuses this. An administrator adds protection-key to the domain’s auth Secret; until then, choose
            partial or withheld for these kinds.
          </span>
        </p>
      ) : null}
      {changed && locked ? (
        <p role="alert" className="border-t border-[var(--color-line)] px-3 py-2.5 text-[12px] text-[var(--color-phase-failed)]">
          {lockText}{' '}
          <button type="button" className="underline" onClick={() => onChange(defaultChoice)}>
            {resetLabel}
          </button>
        </p>
      ) : null}
      {error ? <p className="border-t border-[var(--color-line)] px-3 py-2.5 text-[12px] text-[var(--color-phase-failed)]">{error}</p> : null}
    </section>
  );
}
