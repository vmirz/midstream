import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';

import { applySettingOverrides } from '../config.ts';
import { federationKeysAgree } from '../routes/system.ts';
import type { CpSystemSettings } from '../../shared/types.ts';
import { compatibleControlPlane, liveTailConfig, toDefaults, type Domain } from './domain.ts';
import { loadRegistry, RegistryError, serviceTokenVariable, validateRegistry } from './registry.ts';
import { installTestDomains } from './testing.ts';

const domains = await installTestDomains(['domain_a', 'domain_b'], { entries: { domain_b: { tailUrl: null } } });
const a = domains.require('domain_a');
const b = domains.require('domain_b');
const settings = a.controlPlane.settings.bind(a.controlPlane);

afterEach(() => {
  applySettingOverrides({});
  a.controlPlane.settings = settings;
  a.invalidateSystemSettings();
});

describe('the registry', () => {
  const entry = { id: 'domain_a', name: 'Domain A', controlPlaneUrl: 'http://cp-a:6065/', settingsHome: true };

  it('reads each domain, with its token from a variable of its own and never from the registry', () => {
    const [domain] = validateRegistry([{ ...entry, tailUrl: 'https://a-api.example.com/' }], { [serviceTokenVariable('domain_a')]: ' token-a ' });
    assert.deepEqual(domain, {
      id: 'domain_a',
      name: 'Domain A',
      local: false,
      settingsHome: true,
      controlPlaneUrl: 'http://cp-a:6065',
      tailUrl: 'https://a-api.example.com',
      serviceToken: 'token-a',
    });
    assert.equal(serviceTokenVariable('domain_a'), 'CONSOLE_DOMAIN_DOMAIN_A_SERVICE_TOKEN');
  });

  it('refuses what the chart refuses', () => {
    const refuses = (raw: unknown, message: RegExp) => assert.throws(() => validateRegistry(raw, {}), (error: Error) => error instanceof RegistryError && message.test(error.message));
    refuses([], /names no domain/);
    refuses([{ ...entry, id: 'Domain-A' }], /cannot be a domain id/);
    refuses([{ ...entry, id: 'catalog' }], /cannot be a domain id/);
    refuses([{ ...entry, id: 'a_iceberg' }], /cannot be a domain id/);
    refuses([entry, { ...entry, settingsHome: false }], /named twice/);
    refuses([{ ...entry, settingsHome: false }], /exactly one domain must be the settings home/);
    refuses([entry, { ...entry, id: 'domain_b' }], /exactly one domain/);
    refuses([{ ...entry, controlPlaneUrl: '' }], /controlPlaneUrl is required/);
    refuses([{ ...entry, controlPlaneUrl: 'ftp://cp' }], /http or https/);
    refuses([{ ...entry, tailUrl: 'not a url' }], /not a URL/);
  });

  it('takes the chart’s file, or the same JSON inline, or one local domain for development', () => {
    const inline = loadRegistry({ json: JSON.stringify([entry]) }, {});
    assert.equal(inline[0]?.id, 'domain_a');
    const development = loadRegistry({}, {});
    assert.deepEqual(
      development.map((domain) => [domain.id, domain.controlPlaneUrl, domain.settingsHome]),
      [['local', 'http://localhost:6065', true]],
    );
    assert.throws(() => loadRegistry({ file: '/nonexistent/domains.json' }, {}), /cannot read the domains file/);
    assert.throws(() => loadRegistry({ json: '{not json' }, {}), /not JSON/);
  });
});

describe('a domain’s control plane', () => {
  it('needs 1.0 or later, and cannot say for an unknown version', () => {
    assert.equal(compatibleControlPlane('1.0.0'), true);
    assert.equal(compatibleControlPlane('1.0.0rc1'), true, 'a release candidate reports the release it leads to');
    assert.equal(compatibleControlPlane('1.2.1'), true);
    assert.equal(compatibleControlPlane('0.9.0'), false, 'nothing before the first release');
    assert.equal(compatibleControlPlane(null), null);
    assert.equal(compatibleControlPlane('dev'), null);
  });

  it('is served while it says it is the domain the registry says', async () => {
    assert.equal(a.status, 'ok');
    assert.equal(a.served, true);
    assert.equal(a.version, '1.0.0');
    assert.equal(a.federationKeyId, 'abcd1234');
  });

  it('is misconfigured, and served nothing, when it says it is another', async () => {
    const wrong = await installTestDomains(['domain_a', 'domain_b'], { reports: { domain_b: 'domain_c' } });
    try {
      const domain = wrong.require('domain_b');
      assert.equal(domain.status, 'misconfigured');
      assert.equal(domain.served, false);
      assert.match(domain.statusMessage ?? '', /domain_c/);
      assert.deepEqual(wrong.served().map((served) => served.id), ['domain_a']);
    } finally {
      await installTestDomains(['domain_a', 'domain_b'], { entries: { domain_b: { tailUrl: null } } });
    }
  });

  it('is unreachable while its latest check fails, and ok again once it answers; misconfigured stays so', async () => {
    const [domain] = (await installTestDomains(['domain_a'])).all() as [Domain];
    try {
      const answering = domain.controlPlane.request.bind(domain.controlPlane);
      let down = true;
      domain.controlPlane.request = (async (...args: Parameters<typeof answering>) => {
        if (down) throw new Error('control plane at http://domain-a.test:6065 is unreachable');
        return answering(...args);
      }) as typeof domain.controlPlane.request;
      await domain.checkIdentity();
      assert.equal(domain.status, 'unreachable', 'it answered once, but not now');
      assert.match(domain.statusMessage ?? '', /cannot be reached: control plane at http:\/\/domain-a\.test:6065 is unreachable/);
      assert.equal(domain.served, true);
      down = false;
      await domain.checkIdentity();
      assert.equal(domain.status, 'ok');

      const wrong = (await installTestDomains(['domain_a'], { reports: { domain_a: 'domain_z' } })).require('domain_a');
      wrong.controlPlane.request = (async () => {
        throw new Error('gone');
      }) as typeof wrong.controlPlane.request;
      await wrong.checkIdentity();
      assert.equal(wrong.status, 'misconfigured', 'last seen as another domain: nothing of it is served even while it cannot be reached');
      assert.equal(wrong.served, false);
    } finally {
      await installTestDomains(['domain_a', 'domain_b'], { entries: { domain_b: { tailUrl: null } } });
    }
  });

  it('is unreachable until it answers, and serving it is not refused for that', async () => {
    const [unreachable] = (await installTestDomains(['domain_a'])).all() as [Domain];
    (unreachable as unknown as { identity: null }).identity = null;
    assert.equal(unreachable.status, 'unreachable');
    assert.equal(unreachable.served, true, 'its own requests fail as they come; nothing else is withheld');
    await installTestDomains(['domain_a', 'domain_b'], { entries: { domain_b: { tailUrl: null } } });
  });
});

describe('federation keys across domains', () => {
  it('agree when every reachable domain reports the same key', () => {
    const domain = (_id: string, key: string | null, status = 'ok') => ({ federationKeyId: key, status }) as unknown as Domain;
    assert.equal(federationKeysAgree([domain('a', 'k1'), domain('b', 'k1')]), true);
    assert.equal(federationKeysAgree([domain('a', 'k1'), domain('b', 'k2')]), false);
    assert.equal(federationKeysAgree([domain('a', 'k1'), domain('b', null)]), null, 'one domain alone cannot disagree');
    assert.equal(federationKeysAgree([domain('a', 'k1'), domain('b', 'k1'), domain('c', null)]), false, 'a reachable domain with no key cannot verify a token');
  });
});

describe('the live tail of a domain', () => {
  it('is on where its tail app is configured, unless this console or its control plane turned it off', () => {
    assert.deepEqual(liveTailConfig('http://tail:6065', true), { configured: true, enabled: true, refreshMs: 1_500, maxRows: 100_000 });
    assert.deepEqual(liveTailConfig('http://tail:6065', null), { configured: true, enabled: true, refreshMs: 1_500, maxRows: 100_000 }, 'a control plane that cannot say does not turn it off');
    assert.deepEqual(liveTailConfig('http://tail:6065', false), { configured: true, enabled: false, refreshMs: 1_500, maxRows: 100_000 });
    assert.deepEqual(liveTailConfig(null, true), { configured: false, enabled: false, refreshMs: 1_500, maxRows: 100_000 });
    applySettingOverrides({ liveTailEnabled: false, liveTailRefreshMs: 4_000 });
    assert.deepEqual(liveTailConfig('http://tail:6065', true), { configured: true, enabled: false, refreshMs: 4_000, maxRows: 100_000 });
  });

  it('names the switch that is off: every materialization has a live tail', async () => {
    a.controlPlane.settings = async () => ({ live_tail_enabled: true }) as CpSystemSettings;
    assert.equal(await a.liveTailOff({ live_tail: true }), null);
    assert.equal(await a.liveTailOff({}), null, 'whether or not the control plane says so');
    assert.equal((await a.liveTailOff({ live_tail: false }))?.reason, 'materialization', 'a control plane before 4.3 may still say one is off');
    assert.equal(await a.liveTailOff({ live_tail: true, desired_phase: 'running' }), null);
    assert.equal((await a.liveTailOff({ live_tail: true, desired_phase: 'stopped' }))?.reason, 'stopped', 'the control plane tails a running one only');
    assert.equal((await a.liveTailOff({ live_tail: true, desired_phase: 'deleting' }))?.reason, 'stopped');
    assert.equal((await b.liveTailOff({ live_tail: true }))?.reason, 'not_configured', 'a domain with no tail app');

    a.invalidateSystemSettings();
    a.controlPlane.settings = async () => ({ live_tail_enabled: false }) as CpSystemSettings;
    assert.equal((await a.liveTailOff({ live_tail: true }))?.reason, 'control_plane');

    applySettingOverrides({ liveTailEnabled: false });
    assert.equal((await a.liveTailOff({ live_tail: true }))?.reason, 'console');
  });
});

describe('runtime autoscaling of a domain', () => {
  it('is its control plane’s: whether it scales runtimes at all, how far, and for what catch-up time', async () => {
    a.controlPlane.settings = async () =>
      ({ autoscaling_enabled: true, autoscale_max_replicas: 16, autoscale_target_minutes: 30, max_replicas: 64 }) as CpSystemSettings;
    assert.deepEqual(await a.autoscaling(), { enabled: true, maxReplicas: 16, targetMinutes: 30 });

    a.invalidateSystemSettings();
    a.controlPlane.settings = async () => ({ autoscaling_enabled: false, autoscale_max_replicas: 8, autoscale_target_minutes: 15 }) as CpSystemSettings;
    assert.deepEqual(await a.autoscaling(), { enabled: false, maxReplicas: 8, targetMinutes: 15 });
  });

  it('is unknown for a control plane that cannot be reached', async () => {
    a.controlPlane.settings = async () => {
      throw new Error('unreachable');
    };
    assert.equal(await a.autoscaling(), null);
  });

  it('keeps each domain’s settings apart', async () => {
    a.controlPlane.settings = async () => ({ autoscaling_enabled: true, max_replicas: 4 }) as CpSystemSettings;
    b.controlPlane.settings = async () => ({ autoscaling_enabled: false, max_replicas: 9 }) as CpSystemSettings;
    assert.equal((await a.autoscaling())?.enabled, true);
    assert.equal((await b.autoscaling())?.enabled, false);
  });
});

describe('a create’s defaults', () => {
  it('do not choose a live tail', () => {
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
