/**
 * Route tests: domains with no control plane behind them. Each domain's client answers
 * `GET /system/version` as that domain, so the console serves it; a test stubs any other method it
 * needs on `domain.controlPlane` or `domain.live`. Not used by the running console.
 */

import { SERVICE } from '../controlPlane/client.ts';
import { Domains, useDomains } from './index.ts';
import type { DomainConfig } from './registry.ts';

/**
 * Domains `ids`, the first the settings home, installed as the console's. Each answers as itself,
 * unless `reports` names another id it says it is - a misconfigured registry.
 */
export async function installTestDomains(
  ids: readonly string[] = ['domain_a'],
  options: { entries?: Partial<Record<string, Partial<DomainConfig>>>; reports?: Record<string, string> } = {},
): Promise<Domains> {
  const overrides = options.entries ?? {};
  const entries: DomainConfig[] = ids.map((id, index) => ({
    id,
    name: `Domain ${id.replace(/^domain_/, '').toUpperCase()}`,
    local: index === 0,
    settingsHome: index === 0,
    controlPlaneUrl: `http://${id.replace(/_/g, '-')}.test:6065`,
    tailUrl: `http://${id.replace(/_/g, '-')}-tail.test:6065`,
    serviceToken: `service-token-${id}`,
    ...overrides[id],
  }));
  const domains = useDomains(new Domains(entries));
  for (const domain of domains.all()) {
    const request = domain.controlPlane.request.bind(domain.controlPlane);
    const reported = options.reports?.[domain.id] ?? domain.id;
    domain.controlPlane.request = (async (caller, path, options) => {
      if (caller === SERVICE && path === '/system/version') {
        return { component: 'control-plane', version: '1.0.0', domain: { id: reported, name: domain.name }, federation_key_id: 'abcd1234' };
      }
      return request(caller, path, options);
    }) as typeof domain.controlPlane.request;
    await domain.ready();
  }
  return domains;
}
