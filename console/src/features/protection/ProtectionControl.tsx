import { Shield } from 'lucide-react';
import type { ReactNode } from 'react';

import { cn } from '@/lib/cn';

import { MASK_LEGEND, MASK_NAMES } from './protection';

/** One kind of sensitive data the control offers. */
export interface ProtectionOption {
  value: string;
  label: string;
  description: string;
  /** The masks it allows, in order. */
  choices: string[];
  /** What a partial mask makes of a typical value, when known. */
  partialExample?: { value: string; partial: string } | null;
}

/**
 * Choose data protection: a row per kind of sensitive data, each with Off and the masks it allows.
 * Shared by Settings › Data protection, the create dialog and the edit dialog (handbook 22).
 */
export function ProtectionControl({
  options,
  value,
  onChange,
  labels,
  disabled = false,
  idPrefix,
}: {
  options: ProtectionOption[];
  value: Record<string, string>;
  onChange: (value: Record<string, string>) => void;
  /** What each mask is called; the console's own names otherwise. */
  labels?: Record<string, string> | null;
  disabled?: boolean;
  idPrefix: string;
}): ReactNode {
  const nameOf = (mask: string): string => labels?.[mask] ?? MASK_NAMES[mask as keyof typeof MASK_NAMES] ?? mask;
  return (
    <div className="space-y-2">
      <ul className="divide-y divide-[var(--color-line)] rounded-[var(--radius-control)] border border-[var(--color-line)]">
        {options.map((option) => {
          const current = value[option.value] ?? '';
          const choices = ['', ...option.choices];
          const labelId = `${idPrefix}-${option.value}`;
          return (
            <li key={option.value} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-2.5">
              <div className="min-w-0 flex-1">
                <div id={labelId} className="flex items-center gap-1.5 text-[13px] font-medium">
                  {current ? <Shield className="size-3.5 shrink-0 text-[var(--color-accent)]" aria-hidden /> : null}
                  {option.label}
                </div>
                <div className="text-[12px] text-[var(--color-ink-muted)]">
                  {option.description}
                  {current === 'partial' && option.partialExample ? (
                    <span className="numeric ml-1 text-[var(--color-ink-faint)]">
                      · {option.partialExample.value} → {option.partialExample.partial}
                    </span>
                  ) : null}
                </div>
              </div>
              <div
                role="radiogroup"
                aria-labelledby={labelId}
                className={cn(
                  'inline-flex shrink-0 items-center gap-0.5 rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] p-0.5',
                  disabled && 'opacity-50',
                )}
              >
                {choices.map((choice) => {
                  const selected = current === choice;
                  return (
                    <button
                      key={choice || 'off'}
                      type="button"
                      role="radio"
                      aria-checked={selected}
                      disabled={disabled}
                      onClick={() => {
                        const next = { ...value };
                        if (choice) next[option.value] = choice;
                        else delete next[option.value];
                        onChange(next);
                      }}
                      className={cn(
                        'rounded-[6px] px-2.5 py-1 text-[12px] font-medium transition-colors disabled:cursor-not-allowed',
                        selected
                          ? 'bg-[var(--color-surface)] text-[var(--color-ink)] shadow-sm'
                          : 'text-[var(--color-ink-muted)] enabled:hover:text-[var(--color-ink)]',
                      )}
                    >
                      {choice ? nameOf(choice) : 'Off'}
                    </button>
                  );
                })}
              </div>
            </li>
          );
        })}
      </ul>
      <p className="text-[12px] text-[var(--color-ink-faint)]">{MASK_LEGEND}</p>
    </div>
  );
}
