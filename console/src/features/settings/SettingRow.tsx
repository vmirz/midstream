import { AlertTriangle, RotateCcw, Undo2 } from 'lucide-react';
import type { ReactNode } from 'react';

import { RelativeTime } from '@/components/Inline';
import { Badge, FIELD_BASE, Select } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import type { SettingView } from '@shared/types';

import {
  draftChanges,
  draftError,
  fallbackSource,
  fallbackValue,
  formatNumber,
  formatValue,
  initialInput,
  inputHint,
  unitLabel,
  type Draft,
} from './values';

/** On/off control for boolean settings. */
export function Switch({
  checked,
  disabled,
  onChange,
  label,
  danger = false,
}: {
  checked: boolean;
  disabled?: boolean;
  onChange: (value: boolean) => void;
  label: string;
  danger?: boolean;
}): ReactNode {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={cn(
        'relative inline-flex h-5 w-9 shrink-0 items-center rounded-full border transition-colors',
        'disabled:cursor-not-allowed disabled:opacity-40',
        checked
          ? danger
            ? 'border-transparent bg-[var(--color-phase-failed)]'
            : 'border-transparent bg-[var(--color-accent)]'
          : 'border-[var(--color-line-strong)] bg-[var(--color-raised)]',
      )}
    >
      <span
        className={cn(
          'inline-block size-3.5 rounded-full bg-white shadow-sm transition-transform',
          checked ? 'translate-x-[18px]' : 'translate-x-[2px]',
        )}
      />
    </button>
  );
}

/**
 * One setting: what it is, the control that changes it, and where its value comes from.
 *
 * Edits are drafts until the page is saved. A reset returns the setting to the value the
 * deployment sets, or to the built-in default when the deployment sets none.
 */
export function SettingRow({
  setting,
  draft,
  readOnly,
  serverError,
  onDraft,
}: {
  setting: SettingView;
  draft: Draft | undefined;
  readOnly: boolean;
  /** A problem the server reported with this setting on the last save. */
  serverError: string | null;
  onDraft: (draft: Draft | undefined) => void;
}): ReactNode {
  const input = draft && !draft.reset ? draft.input : initialInput(setting);
  const pendingReset = draft?.reset === true;
  const changed = draftChanges(setting, draft);
  const error = draftError(setting, draft) ?? serverError;
  const hint = pendingReset ? null : inputHint(setting, input);
  const unit = unitLabel(setting);
  const id = `setting-${setting.scope}-${setting.key}`;

  const control = (() => {
    if (setting.kind === 'boolean') {
      const checked = pendingReset ? fallbackValue(setting) === true : input === true;
      return (
        <Switch
          checked={checked}
          disabled={readOnly}
          danger={setting.danger}
          label={setting.label}
          onChange={(value) => onDraft(value === setting.value ? undefined : { reset: false, input: value })}
        />
      );
    }
    if (setting.kind === 'enum') {
      const value = pendingReset ? String(fallbackValue(setting) ?? '') : String(input);
      return (
        <Select
          id={id}
          value={value}
          disabled={readOnly}
          className="h-8 w-44 text-[13px]"
          onChange={(event) =>
            onDraft(event.target.value === setting.value ? undefined : { reset: false, input: event.target.value })
          }
        >
          {(setting.options ?? []).map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </Select>
      );
    }
    const text = pendingReset ? formatInputValue(fallbackValue(setting)) : String(input);
    const wide = setting.kind === 'gcs_uri' || setting.kind === 'string' || setting.kind === 'domain_list';
    return (
      <div
        className={cn(
          FIELD_BASE,
          'flex h-8 items-center gap-2 pr-2.5 focus-within:border-[var(--color-accent)]',
          wide ? 'w-full md:w-72' : 'w-44',
          error && 'border-[var(--color-phase-failed)] focus-within:border-[var(--color-phase-failed)]',
          readOnly && 'opacity-50',
        )}
      >
        <input
          id={id}
          value={text}
          disabled={readOnly}
          inputMode={setting.kind === 'integer' ? 'numeric' : undefined}
          spellCheck={false}
          placeholder={setting.kind === 'gcs_uri' ? 'None' : setting.kind === 'domain_list' ? 'Any: example.com, corp.example.org' : undefined}
          aria-invalid={error ? true : undefined}
          aria-describedby={`${id}-meta`}
          className={cn(
            'min-w-0 flex-1 bg-transparent text-[13px] outline-none placeholder:text-[var(--color-ink-faint)]',
            setting.kind !== 'string' && 'numeric',
          )}
          onChange={(event) => onDraft({ reset: false, input: event.target.value })}
        />
        {unit ? <span className="shrink-0 text-[12px] text-[var(--color-ink-faint)]">{unit}</span> : null}
      </div>
    );
  })();

  return (
    <div className={cn('grid gap-3 px-5 py-4 md:grid-cols-[minmax(0,1fr)_auto] md:gap-8', changed && 'bg-[var(--color-accent-soft)]/40')}>
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <label htmlFor={setting.kind === 'boolean' ? undefined : id} className="text-[13px] font-medium">
            {setting.label}
          </label>
          {setting.danger ? (
            <Badge tone="paused" title="Changing this can remove data">
              <AlertTriangle className="size-3" />
              Destructive
            </Badge>
          ) : null}
          {changed ? <Badge tone="accent">Unsaved</Badge> : null}
        </div>
        {setting.description ? (
          <p className="mt-1 max-w-2xl text-[12px] leading-relaxed text-[var(--color-ink-muted)]">{setting.description}</p>
        ) : null}
        <SettingMeta
          id={`${id}-meta`}
          setting={setting}
          pendingReset={pendingReset}
          readOnly={readOnly}
          onReset={() => onDraft(setting.source === 'override' ? { reset: true } : undefined)}
          onUndo={() => onDraft(undefined)}
          hasDraft={draft !== undefined}
        />
      </div>
      <div className="flex flex-col items-start gap-1 md:items-end">
        {control}
        {error ? (
          <span role="alert" className="text-[12px] text-[var(--color-phase-failed)]">
            {error}
          </span>
        ) : hint ? (
          <span className="numeric text-[12px] text-[var(--color-ink-faint)]">= {hint}</span>
        ) : null}
      </div>
    </div>
  );
}

/** Where the value comes from, when the change applies, and the way back to the deployment value. */
function SettingMeta({
  id,
  setting,
  pendingReset,
  readOnly,
  hasDraft,
  onReset,
  onUndo,
}: {
  id: string;
  setting: SettingView;
  pendingReset: boolean;
  readOnly: boolean;
  hasDraft: boolean;
  onReset: () => void;
  onUndo: () => void;
}): ReactNode {
  const fallback = formatValue(setting, fallbackValue(setting));
  const fallbackFrom = fallbackSource(setting) === 'deployment' ? 'deployment value' : 'default';
  return (
    <div id={id} className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[12px] text-[var(--color-ink-faint)]">
      {pendingReset ? (
        <span className="text-[var(--color-accent)]">
          Returns to the {fallbackFrom}, {fallback}, when saved
        </span>
      ) : setting.source === 'override' ? (
        <span>
          Changed
          {setting.updatedBy ? ` by ${setting.updatedBy}` : ''}
          {setting.updatedAt ? (
            <>
              {' · '}
              <RelativeTime value={setting.updatedAt} />
            </>
          ) : null}
          {' · '}
          {fallbackFrom} {fallback}
        </span>
      ) : setting.source === 'deployment' ? (
        <span title={setting.envVar ?? undefined}>
          Set by the deployment{setting.envVar ? <code className="numeric ml-1">{setting.envVar}</code> : null}
        </span>
      ) : (
        <span>Default</span>
      )}
      {setting.effect ? <span>Applies to {setting.effect}</span> : null}
      {hasDraft ? (
        <button
          type="button"
          onClick={onUndo}
          className="inline-flex items-center gap-1 text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]"
        >
          <Undo2 className="size-3" />
          Undo
        </button>
      ) : setting.source === 'override' && !readOnly ? (
        <button
          type="button"
          onClick={onReset}
          className="inline-flex items-center gap-1 text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]"
          title={`Return to the ${fallbackFrom}: ${fallback}`}
        >
          <RotateCcw className="size-3" />
          Reset
        </button>
      ) : null}
    </div>
  );
}

function formatInputValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  return typeof value === 'number' ? formatNumber(value) : String(value);
}

export function SettingRowList({ children }: { children: ReactNode }): ReactNode {
  return <div className="divide-y divide-[var(--color-line)]">{children}</div>;
}

/** A setting value written out for history and summaries. */
export function ValueText({ setting, value }: { setting: Pick<SettingView, 'kind' | 'unit'>; value: unknown }): ReactNode {
  return <span className="numeric">{formatValue(setting, value as never)}</span>;
}
