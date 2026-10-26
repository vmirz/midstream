import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';

// A console with a tail app, as every deployment has one.
process.env.CONSOLE_TAIL_URL = 'http://materializer-tail-app-api:6065/';

const { compatibleControlPlane, controlPlaneAutoscaling, invalidateMaterializationDefaults, liveTailConfig, liveTailOff, toDefaults } =
  await import('./system.ts');
const { applySettingOverrides, config } = await import('../config.ts');
const { controlPlane } = await import('../controlPlane/client.ts');

import type { CpSystemSettings } from '../../shared/types.ts';

const settings = controlPlane.settings.bind(controlPlane);

afterEach(() => {
  applySettingOverrides({});
  controlPlane.settings = settings;
  invalidateMaterializationDefaults();
});

describe('control plane compatibility', () => {
  it('needs 1.0 or later, and cannot say for an unknown version', () => {
    assert.equal(compatibleControlPlane('1.0.0'), true);
    assert.equal(compatibleControlPlane('1.0.0rc1'), true, 'a release candidate reports the release it leads to');
    assert.equal(compatibleControlPlane('1.2.1'), true);
    assert.equal(compatibleControlPlane('0.9.0'), false, 'nothing before the first release');
    assert.equal(compatibleControlPlane(null), null);
    assert.equal(compatibleControlPlane('dev'), null);
  });
});

describe('the live tail in /api/config', () => {
  it('is on where a tail app is configured, unless this console or the control plane turned it off', () => {
    assert.equal(config.tailUrl, 'http://materializer-tail-app-api:6065', 'without its trailing slash');
    assert.deepEqual(liveTailConfig(true), { configured: true, enabled: true, refreshMs: 1_500, maxRows: 100_000 });
    assert.deepEqual(liveTailConfig(null), { configured: true, enabled: true, refreshMs: 1_500, maxRows: 100_000 }, 'a control plane that cannot say does not turn it off');
    assert.deepEqual(liveTailConfig(false), { configured: true, enabled: false, refreshMs: 1_500, maxRows: 100_000 });
    applySettingOverrides({ liveTailEnabled: false, liveTailRefreshMs: 4_000 });
    assert.deepEqual(liveTailConfig(true), { configured: true, enabled: false, refreshMs: 4_000, maxRows: 100_000 });
  });
});

describe('runtime autoscaling in /api/config', () => {
  it('is the control plane’s: whether it scales runtimes at all, how far, and for what catch-up time', async () => {
    controlPlane.settings = async () =>
      ({ autoscaling_enabled: true, autoscale_max_replicas: 16, autoscale_target_minutes: 30, max_replicas: 64 }) as CpSystemSettings;
    assert.deepEqual(await controlPlaneAutoscaling(), { enabled: true, maxReplicas: 16, targetMinutes: 30 });

    invalidateMaterializationDefaults();
    controlPlane.settings = async () => ({ autoscaling_enabled: false, autoscale_max_replicas: 8, autoscale_target_minutes: 15 }) as CpSystemSettings;
    assert.deepEqual(await controlPlaneAutoscaling(), { enabled: false, maxReplicas: 8, targetMinutes: 15 });
  });

  it('is unknown for a control plane before 4.4, which never scales a runtime, or one that cannot be reached', async () => {
    controlPlane.settings = async () => ({ live_tail_enabled: true, max_replicas: 64 }) as CpSystemSettings;
    assert.equal(await controlPlaneAutoscaling(), null);

    invalidateMaterializationDefaults();
    controlPlane.settings = async () => {
      throw new Error('unreachable');
    };
    assert.equal(await controlPlaneAutoscaling(), null);
  });
});

describe('why a run reads no tail', () => {
  it('names the switch that is off: every materialization has a live tail', async () => {
    controlPlane.settings = async () => ({ live_tail_enabled: true }) as CpSystemSettings;
    assert.equal(await liveTailOff({ live_tail: true }), null);
    assert.equal(await liveTailOff({}), null, 'whether or not the control plane says so');
    assert.equal((await liveTailOff({ live_tail: false }))?.reason, 'materialization', 'a control plane before 4.3 may still say one is off');
    assert.equal(await liveTailOff({ live_tail: true, desired_phase: 'running' }), null);
    assert.equal((await liveTailOff({ live_tail: true, desired_phase: 'stopped' }))?.reason, 'stopped', 'the control plane tails a running one only');
    assert.equal((await liveTailOff({ live_tail: true, desired_phase: 'deleting' }))?.reason, 'stopped');

    invalidateMaterializationDefaults();
    controlPlane.settings = async () => ({ live_tail_enabled: false }) as CpSystemSettings;
    assert.equal((await liveTailOff({ live_tail: true }))?.reason, 'control_plane');

    applySettingOverrides({ liveTailEnabled: false });
    assert.equal((await liveTailOff({ live_tail: true }))?.reason, 'console');
  });

  it('pre-checks the create dialog’s box from the control plane’s default', () => {
    const base = {
      default_destination_uri: 'gs://lake',
      default_namespace: 'midstream',
      default_replicas: 1,
      max_replicas: 8,
      default_storage_size: '1Gi',
      label_max_length: 128,
      dataset_name_max_length: 63,
      table_name_max_length: 63,
      reserved_names: [],
      reserved_namespace_names: [],
    } as CpSystemSettings;
    assert.equal('liveTail' in toDefaults(base), false, 'a create does not choose its live tail');
  });
});
