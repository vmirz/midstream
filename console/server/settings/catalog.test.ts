import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';

import {
  TUNABLE_KEYS,
  applySettingOverrides,
  config,
  currentSettingOverrides,
  deploymentSettings,
  environmentDefault,
} from '../config.ts';
import {
  CONSOLE_SETTINGS,
  consoleSettingDefinition,
  consoleSettingProblem,
  undescribedTunableKeys,
  validConsoleOverrides,
} from './catalog.ts';

afterEach(() => applySettingOverrides({}));

describe('the console settings catalog', () => {
  it('describes every setting the Settings page can override, and nothing else', () => {
    assert.deepEqual(undescribedTunableKeys(), []);
    assert.deepEqual(
      new Set(CONSOLE_SETTINGS.map((definition) => definition.key)),
      new Set(TUNABLE_KEYS),
    );
  });

  it('has the live tail’s four settings in its section, with their defaults and bounds', () => {
    const live = CONSOLE_SETTINGS.filter((definition) => definition.section === 'live-tail');
    assert.deepEqual(
      live.map((definition) => [definition.key, definition.envVar, deploymentSettings[definition.key], definition.minimum ?? null, definition.maximum ?? null]),
      [
        ['liveTailEnabled', 'CONSOLE_LIVE_TAIL_ENABLED', true, null, null],
        ['liveTailRefreshMs', 'CONSOLE_LIVE_TAIL_REFRESH_MS', 1_500, 500, 60_000],
        ['liveTailMaxRows', 'CONSOLE_LIVE_TAIL_MAX_ROWS', 100_000, 1_000, 10_000_000],
        ['liveTailTimeoutMs', 'CONSOLE_LIVE_TAIL_TIMEOUT_MS', 3_000, 500, 30_000],
      ],
    );
    applySettingOverrides(validConsoleOverrides({ liveTailRefreshMs: 2_500 }).valid);
    assert.equal(config.liveTailRefreshMs, 2_500, 'read where it is used');
    assert.ok(consoleSettingProblem(consoleSettingDefinition('liveTailRefreshMs')!, 100));
  });

  it('names each setting’s environment variable once, as config.ts reads it', () => {
    const envVars = CONSOLE_SETTINGS.map((definition) => definition.envVar);
    assert.equal(new Set(envVars).size, envVars.length);
    for (const definition of CONSOLE_SETTINGS) {
      assert.notEqual(environmentDefault(definition.envVar), undefined, `${definition.envVar} is not read by config.ts`);
    }
  });

  it('says what every primary setting controls', () => {
    for (const definition of CONSOLE_SETTINGS) {
      assert.ok(definition.label, definition.key);
      if (!definition.advanced) assert.ok(definition.description, `${definition.key} needs a description`);
    }
  });

  it('accepts its deployment defaults', () => {
    for (const definition of CONSOLE_SETTINGS) {
      const value = environmentDefault(definition.envVar);
      assert.equal(consoleSettingProblem(definition, value), null, `${definition.key} default ${String(value)}`);
    }
  });

  it('checks values the way the page does', () => {
    const poll = consoleSettingDefinition('listPollIntervalMs')!;
    assert.equal(consoleSettingProblem(poll, 5_000), null);
    assert.match(consoleSettingProblem(poll, 100) ?? '', /at least 500/);
    assert.match(consoleSettingProblem(poll, 1.5) ?? '', /whole number/);
    assert.match(consoleSettingProblem(poll, '5000') ?? '', /whole number/);

    const level = consoleSettingDefinition('logLevel')!;
    assert.equal(consoleSettingProblem(level, 'debug'), null);
    assert.match(consoleSettingProblem(level, 'verbose') ?? '', /one of/);

    const enabled = consoleSettingDefinition('dataAccessEnabled')!;
    assert.equal(consoleSettingProblem(enabled, false), null);
    assert.match(consoleSettingProblem(enabled, 'false') ?? '', /on or off/);

    const name = { ...consoleSettingDefinition('logLevel')!, kind: 'string' as const, maxLength: 96, options: undefined };
    assert.match(consoleSettingProblem(name, '  ') ?? '', /cannot be empty/);
    assert.match(consoleSettingProblem(name, 'x'.repeat(97)) ?? '', /at most 96/);
  });

  it('keeps only stored overrides this console accepts', () => {
    const { valid, rejected } = validConsoleOverrides({
      listPollIntervalMs: 4_000,
      signInAttemptsPerMinute: 20,
      pageRowLimit: 5,
      unknownSetting: true,
      // Retired with sign-in; older consoles may have stored it.
      defaultSubmittedBy: 'ops',
    });
    assert.deepEqual(valid, { listPollIntervalMs: 4_000, signInAttemptsPerMinute: 20 });
    assert.deepEqual(rejected.sort(), ['pageRowLimit', 'unknownSetting']);
  });
});

describe('live settings', () => {
  it('serve an override through config until it is reset', () => {
    const deployed = deploymentSettings.listPollIntervalMs;
    applySettingOverrides({ listPollIntervalMs: deployed + 1_000, exportRowLimit: 5_000 });
    assert.equal(config.listPollIntervalMs, deployed + 1_000);
    assert.equal(config.exportRowLimit, 5_000);
    assert.deepEqual(currentSettingOverrides(), { listPollIntervalMs: deployed + 1_000, exportRowLimit: 5_000 });

    applySettingOverrides({ exportRowLimit: 5_000 });
    assert.equal(config.listPollIntervalMs, deployed);
  });

  it('turn the data explorer off and on while running', () => {
    const deployed = config.dataAccessEnabled;
    applySettingOverrides({ dataAccessEnabled: false });
    assert.equal(config.dataAccessEnabled, false);
    applySettingOverrides({ dataAccessEnabled: true });
    assert.equal(config.dataAccessEnabled, true);
    applySettingOverrides({});
    assert.equal(config.dataAccessEnabled, deployed);
  });

  it('keep a false override rather than falling back to the deployment', () => {
    applySettingOverrides({ dataAccessEnabled: false });
    assert.equal(config.dataAccessEnabled, false);
  });

  it('leave deployment settings fixed', () => {
    assert.throws(() => {
      (config as { port: number }).port = 1;
    }, TypeError);
  });
});
