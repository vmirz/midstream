/**
 * Settings page logic with no React in it: what an edit means, whether it is valid, how a value
 * reads, and the save request a set of edits becomes.
 */

import type {
  SettingValue,
  SettingView,
  SettingsChangeInput,
  SettingsOverview,
  SettingsScope,
} from '@shared/types';

/** Who each scope's settings configure, as a heading reads. */
export const SCOPE_LABELS = {
  'control-plane': 'Control plane',
  runtime: 'Runtimes',
  console: 'This console',
} as const;

/** An unsaved edit: a new value as typed, or a request to return to the deployment value. */
export type Draft = { reset: true } | { reset: false; input: string | boolean };

export type Drafts = Record<string, Draft>;

export function draftKey(setting: Pick<SettingView, 'scope' | 'key'>): string {
  return `${setting.scope}:${setting.key}`;
}

/** The value a setting returns to when it is not overridden. */
export function fallbackValue(setting: SettingView): SettingValue {
  return setting.deploymentSet ? setting.deploymentValue : setting.defaultValue;
}

export function fallbackSource(setting: SettingView): 'deployment' | 'default' {
  return setting.deploymentSet ? 'deployment' : 'default';
}

/** What a control shows before it is edited; large numbers keep their thousands separators. */
export function initialInput(setting: SettingView): string | boolean {
  if (setting.kind === 'boolean') return setting.value === true;
  if (setting.value === null || setting.value === undefined) return '';
  return typeof setting.value === 'number' ? formatNumber(setting.value) : String(setting.value);
}

const GCS_URI = /^gs:\/\/[a-z0-9][a-z0-9._-]*[a-z0-9](\/\S*)?$/;
const QUANTITY = /^\d+(Ki|Mi|Gi|Ti|K|M|G|T)?$/;

export type Parsed = { ok: true; value: SettingValue } | { ok: false; error: string };

/** The value an edit stands for, checked the way the server checks it. */
export function parseInput(setting: SettingView, input: string | boolean): Parsed {
  const label = setting.label;
  switch (setting.kind) {
    case 'boolean':
      return typeof input === 'boolean' ? { ok: true, value: input } : { ok: false, error: `${label} must be on or off` };
    case 'integer': {
      const text = String(input).trim().replace(/[,_\s]/g, '');
      if (!/^-?\d+$/.test(text)) return { ok: false, error: 'Enter a whole number' };
      const value = Number(text);
      if (!Number.isSafeInteger(value)) return { ok: false, error: 'That number is too large' };
      if (setting.minimum !== null && value < setting.minimum) {
        return { ok: false, error: `At least ${formatNumber(setting.minimum)}` };
      }
      if (setting.maximum !== null && value > setting.maximum) {
        return { ok: false, error: `At most ${formatNumber(setting.maximum)}` };
      }
      return { ok: true, value };
    }
    case 'gcs_uri': {
      const text = String(input).trim().replace(/\/+$/, '');
      if (!text) {
        return setting.nullable ? { ok: true, value: null } : { ok: false, error: `${label} is required` };
      }
      if (text.length > 1024 || !GCS_URI.test(text)) {
        return { ok: false, error: 'A gs:// URI such as gs://bucket/prefix' };
      }
      return { ok: true, value: text };
    }
    case 'quantity': {
      const text = String(input).trim();
      if (!QUANTITY.test(text)) return { ok: false, error: 'A Kubernetes quantity such as 1Gi, 512Mi or 10G' };
      return { ok: true, value: text };
    }
    case 'enum': {
      const text = String(input);
      return (setting.options ?? []).includes(text)
        ? { ok: true, value: text }
        : { ok: false, error: `One of ${(setting.options ?? []).join(', ')}` };
    }
    case 'string': {
      const text = String(input).trim();
      if (!text) return { ok: false, error: `${label} cannot be empty` };
      if (setting.maxLength !== null && text.length > setting.maxLength) {
        return { ok: false, error: `At most ${setting.maxLength} characters` };
      }
      return { ok: true, value: text };
    }
  }
}

/** Whether an edit would change anything; a typed value equal to the current one does not. */
export function draftChanges(setting: SettingView, draft: Draft | undefined): boolean {
  if (!draft) return false;
  if (draft.reset) return setting.source === 'override';
  const parsed = parseInput(setting, draft.input);
  return !parsed.ok || parsed.value !== setting.value;
}

/** The problem with an edit, or null when it can be saved. */
export function draftError(setting: SettingView, draft: Draft | undefined): string | null {
  if (!draft || draft.reset) return null;
  const parsed = parseInput(setting, draft.input);
  return parsed.ok ? null : parsed.error;
}

/** Every setting on the page, by draft key. */
export function settingsByKey(overview: SettingsOverview | undefined): Map<string, SettingView> {
  const settings = new Map<string, SettingView>();
  if (!overview) return settings;
  for (const scope of Object.values(overview.scopes)) {
    for (const setting of scope?.settings ?? []) settings.set(draftKey(setting), setting);
  }
  return settings;
}

/** The edits that change something, and whether any of them cannot be saved. */
export function pendingDrafts(
  drafts: Drafts,
  settings: Map<string, SettingView>,
): { changed: [SettingView, Draft][]; invalid: number } {
  const changed: [SettingView, Draft][] = [];
  let invalid = 0;
  for (const [key, draft] of Object.entries(drafts)) {
    const setting = settings.get(key);
    if (!setting || !draftChanges(setting, draft)) continue;
    changed.push([setting, draft]);
    if (draftError(setting, draft)) invalid += 1;
  }
  return { changed, invalid };
}

/** The save request a set of valid edits becomes: one change per scope, against its revision. */
export function buildChanges(changed: [SettingView, Draft][], overview: SettingsOverview): SettingsChangeInput[] {
  const byScope = new Map<SettingsScope, SettingsChangeInput>();
  for (const [setting, draft] of changed) {
    const scope = overview.scopes[setting.scope];
    let change = byScope.get(setting.scope);
    if (!change) {
      change = { scope: setting.scope, values: {}, reset: [], baseRevision: scope?.revision ?? 0 };
      byScope.set(setting.scope, change);
    }
    if (draft.reset) {
      change.reset.push(setting.key);
      continue;
    }
    const parsed = parseInput(setting, draft.input);
    if (parsed.ok) change.values[setting.key] = parsed.value;
  }
  return [...byScope.values()];
}

/**
 * What the page says when a save is refused over a conflict. A change made since the page loaded
 * gets one plain sentence rather than the server's revision numbers; any other refusal keeps the
 * server's reason, since that is what needs fixing.
 */
export function conflictNotice(message: string, reason: string | undefined): string {
  return reason === 'settings_changed'
    ? 'These settings changed after this page loaded. Your edits are kept: reload to see the current values, then save again.'
    : `${message} Your edits are kept.`;
}

const NUMBER = new Intl.NumberFormat('en-US');

export function formatNumber(value: number): string {
  return NUMBER.format(value);
}

/** A duration in milliseconds, in the largest units that keep it exact enough to read. */
export function humanizeMs(ms: number): string {
  if (ms < 1000) return `${ms} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${trim(seconds)} s`;
  const minutes = seconds / 60;
  if (minutes < 60) return `${trim(minutes)} min`;
  const hours = minutes / 60;
  if (hours < 48) return `${trim(hours)} h`;
  return `${trim(hours / 24)} days`;
}

export function humanizeSeconds(seconds: number): string {
  return humanizeMs(seconds * 1000);
}

const BINARY_UNITS = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB'];

/** Bytes in binary units, which is how the defaults are chosen (8 MiB, 2 GiB). */
export function humanizeBytes(bytes: number): string {
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < BINARY_UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${trim(value)} ${BINARY_UNITS[unit]}`;
}

function trim(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(1).replace(/\.0$/, '');
}

const UNIT_LABELS: Record<string, string> = {
  ms: 'ms',
  seconds: 's',
  minutes: 'min',
  hours: 'h',
  days: 'days',
  bytes: 'bytes',
};

/** The short unit shown beside a number field. */
export function unitLabel(setting: SettingView): string | null {
  if (!setting.unit) return null;
  return UNIT_LABELS[setting.unit] ?? setting.unit;
}

/** A value as prose: what a setting is, or would be, set to. */
export function formatValue(setting: Pick<SettingView, 'kind' | 'unit'>, value: SettingValue): string {
  if (value === null || value === undefined || value === '') {
    return setting.kind === 'gcs_uri' ? 'None' : '—';
  }
  if (typeof value === 'boolean') return value ? 'On' : 'Off';
  if (typeof value === 'number') {
    if (setting.unit === 'ms') return humanizeMs(value);
    if (setting.unit === 'seconds') return humanizeSeconds(value);
    if (setting.unit === 'minutes') return humanizeMs(value * 60_000);
    if (setting.unit === 'hours') return value === 1 ? '1 hour' : `${formatNumber(value)} hours`;
    if (setting.unit === 'days') return value === 1 ? '1 day' : `${formatNumber(value)} days`;
    if (setting.unit === 'attempts' && value === 1) return '1 attempt';
    if (setting.unit === 'bytes') return humanizeBytes(value);
    return setting.unit ? `${formatNumber(value)} ${setting.unit}` : formatNumber(value);
  }
  return String(value);
}

/** A readable equivalent shown under a number field, when the raw number is hard to read. */
export function inputHint(setting: SettingView, input: string | boolean): string | null {
  if (setting.kind !== 'integer' || typeof input !== 'string') return null;
  const parsed = parseInput(setting, input);
  if (!parsed.ok || typeof parsed.value !== 'number') return null;
  if (setting.unit === 'ms' && parsed.value >= 1000) return humanizeMs(parsed.value);
  if (setting.unit === 'seconds' && parsed.value >= 60) return humanizeSeconds(parsed.value);
  if (setting.unit === 'minutes' && parsed.value >= 60) return humanizeMs(parsed.value * 60_000);
  if (setting.unit === 'hours' && parsed.value >= 48) return `${trim(parsed.value / 24)} days`;
  if (setting.unit === 'bytes' && parsed.value >= 1024) return humanizeBytes(parsed.value);
  return null;
}

/** The settings a section shows, primary ones first, in catalog order within each scope. */
export function sectionSettings(overview: SettingsOverview | undefined, section: string): SettingView[] {
  if (!overview) return [];
  const order: SettingsScope[] = ['control-plane', 'runtime', 'console'];
  return order.flatMap((scope) => (overview.scopes[scope]?.settings ?? []).filter((setting) => setting.section === section));
}
