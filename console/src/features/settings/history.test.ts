import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { SettingView, SettingsHistory, SettingsHistoryEntry, SettingsOverview } from '@shared/types';

import { mergeHistories, placeOf, whereFrom } from './history.ts';

function entry(scope: SettingsHistoryEntry['scope'], revision: number, updatedAt: string, key = 'listPollIntervalMs'): SettingsHistoryEntry {
  return {
    scope,
    revision,
    requestId: `req-${revision}`,
    updatedAt,
    updatedBy: 'admin@example.com',
    changes: [{ key, before: 1, after: 2, beforeSource: 'default', afterSource: 'override' }],
  };
}

function history(changes: SettingsHistoryEntry[], errors: SettingsHistory['errors'] = []): SettingsHistory {
  return { changes, retainedPerScope: 100, errors };
}

function overview(settings: Array<Pick<SettingView, 'scope' | 'key' | 'label'>>): SettingsOverview {
  const scopes: SettingsOverview['scopes'] = { 'control-plane': null, runtime: null, console: null };
  for (const setting of settings) {
    const view = (scopes[setting.scope] ??= { scope: setting.scope, revision: 1, updatedAt: null, updatedBy: null, settings: [] });
    view.settings.push(setting as SettingView);
  }
  return { scopes } as SettingsOverview;
}

describe('mergeHistories', () => {
  it('merges the console’s and every domain’s changes, newest first, each saying where it was made', () => {
    const merged = mergeHistories([
      { domain: null, domainName: null, history: history([entry('console', 3, '2026-10-26T10:00:00Z')]), overview: null, error: null },
      { domain: 'domain_a', domainName: 'Domain A', history: history([entry('runtime', 7, '2026-10-26T11:00:00Z')]), overview: null, error: null },
      { domain: 'domain_b', domainName: 'Domain B', history: history([entry('control-plane', 2, '2026-10-26T09:00:00Z')]), overview: null, error: null },
    ]);
    assert.deepEqual(
      merged.entries.map((row) => placeOf(row, true)),
      ['Domain A · Runtimes', 'This console', 'Domain B · Control plane'],
    );
    assert.deepEqual(
      merged.entries.map((row) => whereFrom(row, true)),
      ['Domain A', 'This console', 'Domain B'],
    );
    assert.equal(merged.retainedPerScope, 100);
  });

  it('labels each change from its own domain’s settings, never another’s', () => {
    const merged = mergeHistories([
      {
        domain: 'domain_a',
        domainName: 'Domain A',
        history: history([entry('runtime', 1, '2026-10-26T11:00:00Z', 'batchRows')]),
        overview: overview([{ scope: 'runtime', key: 'batchRows', label: 'Rows per batch' }]),
        error: null,
      },
    ]);
    assert.equal(merged.settings.get('domain_a:runtime:batchRows')?.label, 'Rows per batch');
    assert.equal(merged.settings.get('domain_b:runtime:batchRows'), undefined);
  });

  it('names what could not be read', () => {
    const merged = mergeHistories([
      { domain: 'domain_a', domainName: 'Domain A', history: null, overview: null, error: 'connection refused' },
      { domain: 'domain_b', domainName: 'Domain B', history: history([], [{ scope: 'runtime', message: 'timed out' }]), overview: null, error: null },
    ]);
    assert.deepEqual(merged.errors, ['Domain A (connection refused)', 'Domain B · Runtimes (timed out)']);
  });

  it('on a domain’s own page, places a change by its scope alone', () => {
    const merged = mergeHistories([{ domain: 'domain_a', domainName: 'Domain A', history: history([entry('runtime', 1, '2026-10-26T11:00:00Z')]), overview: null, error: null }]);
    assert.equal(placeOf(merged.entries[0]!, false), 'Runtimes');
    assert.equal(whereFrom(merged.entries[0]!, false), 'Runtimes');
  });
});
