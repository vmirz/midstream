/** How the console's domains are (handbook 20), in words and in agreement. */

import type { ConsoleDomain } from '@shared/types';

/** A domain's status in words. */
export function domainStatusText(domain: Pick<ConsoleDomain, 'status' | 'connected' | 'statusMessage'>): string {
  if (domain.statusMessage) return domain.statusMessage;
  if (domain.status === 'ok') return domain.connected ? 'Live' : 'Reconnecting';
  if (domain.status === 'unreachable') return 'Its control plane cannot be reached';
  if (domain.status === 'misconfigured') return 'Its control plane says it is another domain';
  return 'Its control plane runs a release this console does not work with';
}

/**
 * The key ids that are not the one most domains share: a token one of those mints is refused by the
 * others, and theirs by it. Ties name every key but the first seen.
 */
export function strayKeys(domains: Array<Pick<ConsoleDomain, 'federationKeyId'>>): Set<string> {
  const counts = new Map<string, number>();
  for (const domain of domains) {
    if (domain.federationKeyId) counts.set(domain.federationKeyId, (counts.get(domain.federationKeyId) ?? 0) + 1);
  }
  if (counts.size < 2) return new Set();
  let common: string | null = null;
  for (const [id, count] of counts) if (common === null || count > (counts.get(common) ?? 0)) common = id;
  return new Set([...counts.keys()].filter((id) => id !== common));
}
