/**
 * The change history of settings, merged: one domain's control-plane and runtime scopes on its own
 * page; on the console's, the console's own settings and every domain's (handbook 20).
 */

import type { SettingView, SettingsHistory, SettingsHistoryEntry, SettingsOverview } from '@shared/types';

import { SCOPE_LABELS } from './values.ts';

/** A change, with the domain whose control plane stores it: null for the console's own settings. */
export interface HistoryRow extends SettingsHistoryEntry {
  domain: string | null;
  domainName: string | null;
}

export interface MergedHistory {
  entries: HistoryRow[];
  /** Each setting by `<domain>:<scope>:<key>`, for its label and how its values read. */
  settings: Map<string, SettingView>;
  retainedPerScope: number;
  /** What could not be read, each naming its domain or scope. */
  errors: string[];
}

/** Where a change was made, as its badge reads: the scope, and on the console's page its domain. */
export function placeOf(row: HistoryRow, merged: boolean): string {
  if (row.domain === null) return SCOPE_LABELS.console;
  return merged ? `${row.domainName ?? row.domain} · ${SCOPE_LABELS[row.scope]}` : SCOPE_LABELS[row.scope];
}

/** What the filter chooses a change by: its domain on the console's page, its scope on a domain's. */
export function whereFrom(row: HistoryRow, merged: boolean): string {
  if (merged) return row.domain === null ? SCOPE_LABELS.console : (row.domainName ?? row.domain);
  return SCOPE_LABELS[row.scope];
}

/**
 * Several settings histories as one, newest first: each source's changes, and its settings for their
 * labels. Pure: what the section computes, for tests.
 */
export function mergeHistories(
  sources: Array<{ domain: string | null; domainName: string | null; history: SettingsHistory | null; overview: SettingsOverview | null; error: string | null }>,
): MergedHistory {
  const entries: HistoryRow[] = [];
  const settings = new Map<string, SettingView>();
  const errors: string[] = [];
  let retainedPerScope = 0;
  for (const source of sources) {
    const where = source.domainName ?? SCOPE_LABELS.console;
    if (source.error) errors.push(`${where} (${source.error})`);
    for (const error of source.history?.errors ?? []) errors.push(`${where} · ${SCOPE_LABELS[error.scope]} (${error.message})`);
    for (const entry of source.history?.changes ?? []) entries.push({ ...entry, domain: source.domain, domainName: source.domainName });
    retainedPerScope = Math.max(retainedPerScope, source.history?.retainedPerScope ?? 0);
    for (const view of Object.values(source.overview?.scopes ?? {})) {
      for (const setting of view?.settings ?? []) settings.set(`${source.domain ?? ''}:${setting.scope}:${setting.key}`, setting);
    }
  }
  entries.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt) || right.revision - left.revision);
  return { entries, settings, retainedPerScope, errors };
}
