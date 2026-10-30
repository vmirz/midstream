import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { SettingView, SettingsOverview } from '@shared/types';

import {
  buildChanges,
  conflictNotice,
  draftChanges,
  draftError,
  fallbackValue,
  formatValue,
  humanizeBytes,
  humanizeMs,
  inputHint,
  parseInput,
  pendingDrafts,
  sectionSettings,
  settingsByKey,
} from './values.ts';

function setting(overrides: Partial<SettingView> = {}): SettingView {
  return {
    scope: 'control-plane',
    key: 'max_runtime_replicas',
    section: 'materializations',
    label: 'Maximum replicas',
    description: null,
    kind: 'integer',
    unit: 'replicas',
    minimum: 1,
    maximum: 1024,
    options: null,
    maxLength: null,
    nullable: false,
    advanced: false,
    danger: false,
    effect: null,
    envVar: 'CONTROL_PLANE_MAX_RUNTIME_REPLICAS',
    value: 64,
    source: 'default',
    defaultValue: 64,
    deploymentValue: null,
    deploymentSet: false,
    updatedAt: null,
    updatedBy: null,
    ...overrides,
  };
}

function overview(settings: SettingView[]): SettingsOverview {
  const scope = (name: 'control-plane' | 'runtime' | 'console', revision: number) => ({
    scope: name,
    revision,
    updatedAt: null,
    updatedBy: null,
    settings: settings.filter((item) => item.scope === name),
  });
  return {
    generatedAt: '2026-10-08T10:00:00Z',
    controlPlane: { reachable: true, error: null },
    scopes: { 'control-plane': scope('control-plane', 3), runtime: scope('runtime', 1), console: scope('console', 0) },
    deployment: [],
    rollout: null,
    historyRetained: 100,
  };
}

describe('parseInput', () => {
  it('reads whole numbers within bounds, tolerating separators', () => {
    assert.deepEqual(parseInput(setting(), '32'), { ok: true, value: 32 });
    assert.deepEqual(parseInput(setting({ maximum: 1_000_000 }), '250,000'), { ok: true, value: 250_000 });
    assert.equal(parseInput(setting(), '0').ok, false);
    assert.equal(parseInput(setting(), '2000').ok, false);
    assert.equal(parseInput(setting(), '1.5').ok, false);
    assert.equal(parseInput(setting(), '').ok, false);
  });

  it('reads destinations in each of the three forms, empty meaning none when that is allowed', () => {
    const destination = setting({ kind: 'destination_uri', key: 'default_destination_uri', nullable: true, unit: null });
    assert.deepEqual(parseInput(destination, ' gs://lake/data/ '), { ok: true, value: 'gs://lake/data' });
    assert.deepEqual(parseInput(destination, 'gs://lake_eu'), { ok: true, value: 'gs://lake_eu' });
    assert.deepEqual(parseInput(destination, 's3://example-lake/midstream/'), { ok: true, value: 's3://example-lake/midstream' });
    assert.deepEqual(parseInput(destination, 's3://lake'), { ok: true, value: 's3://lake' });
    assert.deepEqual(
      parseInput(destination, 'abfss://lake@exampleacct.dfs.core.windows.net/midstream'),
      { ok: true, value: 'abfss://lake@exampleacct.dfs.core.windows.net/midstream' },
    );
    assert.deepEqual(parseInput(destination, ''), { ok: true, value: null });
    assert.equal(parseInput({ ...destination, nullable: false }, '').ok, false);
  });

  it('refuse destinations the control plane refuses, naming the three forms', () => {
    const destination = setting({ kind: 'destination_uri', key: 'default_destination_uri', nullable: true, unit: null });
    for (const text of [
      's3://ab',
      's3://lake_eu',
      's3://Lake',
      'gs://-lake',
      'az://lake/prefix',
      'wasbs://lake@exampleacct.blob.core.windows.net',
      'abfss://lake@exampleacct.blob.core.windows.net/x',
      'abfss://la--ke@exampleacct.dfs.core.windows.net',
      'abfss://lake@ex.dfs.core.windows.net',
      'file:///tmp/lake',
      `s3://lake/${'x'.repeat(1024)}`,
    ]) {
      const parsed = parseInput(destination, text);
      assert.equal(parsed.ok, false, text);
      if (!parsed.ok) assert.match(parsed.error, /gs:\/\/bucket\/prefix, s3:\/\/bucket\/prefix or abfss:\/\/container@account\.dfs\.core\.windows\.net\/prefix/);
    }
  });

  it('reads Kubernetes quantities, enums and text', () => {
    assert.deepEqual(parseInput(setting({ kind: 'quantity', unit: null }), '2Gi'), { ok: true, value: '2Gi' });
    assert.equal(parseInput(setting({ kind: 'quantity', unit: null }), 'lots').ok, false);
    const level = setting({ kind: 'enum', options: ['info', 'debug'], unit: null });
    assert.deepEqual(parseInput(level, 'debug'), { ok: true, value: 'debug' });
    assert.equal(parseInput(level, 'verbose').ok, false);
    const name = setting({ kind: 'string', maxLength: 5, unit: null });
    assert.deepEqual(parseInput(name, ' ops '), { ok: true, value: 'ops' });
    assert.equal(parseInput(name, 'toolong').ok, false);
  });
});

describe('drafts', () => {
  it('count only edits that change something', () => {
    const replicas = setting({ value: 64 });
    assert.equal(draftChanges(replicas, { reset: false, input: '64' }), false);
    assert.equal(draftChanges(replicas, { reset: false, input: '32' }), true);
    assert.equal(draftChanges(replicas, { reset: false, input: 'x' }), true, 'an invalid edit must block saving');
    assert.equal(draftChanges(replicas, { reset: true }), false, 'nothing to reset without an override');
    assert.equal(draftChanges(setting({ source: 'override' }), { reset: true }), true);
    assert.equal(draftError(replicas, { reset: false, input: 'x' }), 'Enter a whole number');
  });

  it('become one change per scope against that scope’s revision', () => {
    const replicas = setting({ value: 64 });
    const destination = setting({ key: 'default_destination_uri', kind: 'destination_uri', unit: null, nullable: true, value: null, source: 'override' });
    const seal = setting({ scope: 'runtime', key: 'max_open_file_age_seconds', value: 300, unit: 'seconds', section: 'runtimes' });
    const page = overview([replicas, destination, seal]);
    const { changed, invalid } = pendingDrafts(
      {
        'control-plane:max_runtime_replicas': { reset: false, input: '32' },
        'control-plane:default_destination_uri': { reset: true },
        'runtime:max_open_file_age_seconds': { reset: false, input: '120' },
        'console:unknown': { reset: false, input: '1' },
      },
      settingsByKey(page),
    );
    assert.equal(invalid, 0);
    assert.deepEqual(buildChanges(changed, page), [
      { scope: 'control-plane', values: { max_runtime_replicas: 32 }, reset: ['default_destination_uri'], baseRevision: 3 },
      { scope: 'runtime', values: { max_open_file_age_seconds: 120 }, reset: [], baseRevision: 1 },
    ]);
  });
});

describe('conflictNotice', () => {
  it('explains a change made since the page loaded without revision numbers', () => {
    const notice = conflictNotice('these settings changed after they were read (revision 3, now 4)', 'settings_changed');
    assert.ok(notice.startsWith('These settings changed after this page loaded.'));
    assert.ok(!notice.includes('revision'));
  });

  it('keeps the reason for any other refusal', () => {
    assert.equal(
      conflictNotice('Default replicas cannot exceed Maximum replicas.', 'invalid_setting'),
      'Default replicas cannot exceed Maximum replicas. Your edits are kept.',
    );
  });
});

describe('formatting', () => {
  it('reads durations and sizes the way people say them', () => {
    assert.equal(humanizeMs(1_500), '1.5 s');
    assert.equal(humanizeMs(120_000), '2 min');
    assert.equal(humanizeMs(6 * 3_600_000), '6 h');
    assert.equal(humanizeMs(7 * 86_400_000), '7 days');
    assert.equal(humanizeBytes(8 * 1024 * 1024), '8 MiB');
    assert.equal(humanizeBytes(2 * 1024 ** 3), '2 GiB');
  });

  it('writes values with their unit, and absence as None', () => {
    assert.equal(formatValue({ kind: 'integer', unit: 'ms' }, 30_000), '30 s');
    assert.equal(formatValue({ kind: 'integer', unit: 'replicas' }, 16), '16 replicas');
    assert.equal(formatValue({ kind: 'boolean', unit: null }, false), 'Off');
    assert.equal(formatValue({ kind: 'destination_uri', unit: null }, null), 'None');
    assert.equal(formatValue({ kind: 'integer', unit: 'hours' }, 168), '168 hours');
  });

  it('hints at a readable equivalent only where the raw number is hard to read', () => {
    assert.equal(inputHint(setting({ unit: 'ms', maximum: null }), '900000'), '15 min');
    assert.equal(inputHint(setting({ unit: 'bytes', maximum: null }), '8388608'), '8 MiB');
    assert.equal(inputHint(setting({ unit: 'replicas' }), '16'), null);
    assert.equal(inputHint(setting({ unit: 'ms', maximum: null }), 'x'), null);
  });

  it('falls back to the deployment value before the default', () => {
    assert.equal(fallbackValue(setting({ deploymentSet: true, deploymentValue: 32, defaultValue: 64 })), 32);
    assert.equal(fallbackValue(setting({ deploymentSet: false, deploymentValue: null, defaultValue: 64 })), 64);
  });
});

describe('sectionSettings', () => {
  it('lists a section’s settings scope by scope', () => {
    const page = overview([
      setting({ scope: 'console', key: 'topicCatalogTtlMs', section: 'topics' }),
      setting({ key: 'topic_catalog_ttl_seconds', section: 'topics' }),
      setting({ key: 'max_runtime_replicas' }),
    ]);
    assert.deepEqual(
      sectionSettings(page, 'topics').map((item) => `${item.scope}:${item.key}`),
      ['control-plane:topic_catalog_ttl_seconds', 'console:topicCatalogTtlMs'],
    );
  });
});

describe('a choice-map setting (data protection)', () => {
  const protection = setting({
    key: 'default_protection',
    section: 'protection',
    label: 'Protected by default',
    kind: 'choice_map',
    unit: null,
    minimum: null,
    maximum: null,
    nullable: true,
    choiceOptions: [
      { value: 'email', label: 'Email addresses', description: 'emails', choices: ['hash', 'partial', 'redact'] },
      { value: 'phone', label: 'Phone numbers', description: 'phones', choices: ['hash', 'partial', 'redact'] },
      { value: 'secret', label: 'Passwords, tokens and keys', description: 'secrets', choices: ['partial', 'redact'] },
    ],
    choiceLabels: { hash: 'Hashed', partial: 'Partial', redact: 'Withheld' },
    permission: 'settings.manage_protection',
    value: 'email=hash,secret=redact',
    defaultValue: 'email=hash,phone=hash,secret=redact',
    envVar: 'CONTROL_PLANE_DEFAULT_PROTECTION',
  });

  it('stores choices in the options’ order', () => {
    assert.deepEqual(parseInput(protection, 'secret=partial, email=hash'), { ok: true, value: 'email=hash,secret=partial' });
    assert.deepEqual(parseInput(protection, ''), { ok: true, value: '' });
  });

  it('refuses an unknown option and a choice the option does not allow', () => {
    assert.equal(parseInput(protection, 'nope=hash').ok, false);
    const refused = parseInput(protection, 'secret=hash');
    assert.equal(refused.ok, false);
    assert.match(refused.ok ? '' : refused.error, /Passwords, tokens and keys cannot be hash/);
  });

  it('says what is chosen in words', () => {
    assert.equal(formatValue(protection, 'email=hash,secret=redact'), 'Email addresses: hashed · Passwords, tokens and keys: withheld');
    assert.equal(formatValue(protection, ''), 'None');
  });

  it('is unchanged when the same choices come back in another order', () => {
    assert.equal(draftChanges(protection, { reset: false, input: 'secret=redact,email=hash' }), false);
    assert.equal(draftChanges(protection, { reset: false, input: 'email=partial,secret=redact' }), true);
  });
});
