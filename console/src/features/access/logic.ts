/**
 * The access pages' logic that needs no React: permission implications, generated passwords, and
 * telling sessions apart. Kept apart from the components so it can be tested directly.
 */

import type { CpPermissionCatalog, CpPermissionDefinition } from '@shared/types';

export function permissionMap(catalog: CpPermissionCatalog | undefined): Map<string, CpPermissionDefinition> {
  return new Map((catalog?.permissions ?? []).map((permission) => [permission.key, permission]));
}

/**
 * Every permission `granted` amounts to, with what each implies, as the control plane resolves
 * it. `*` is every permission; keys the catalog does not know are dropped.
 */
export function expandPermissions(granted: Iterable<string>, catalog: CpPermissionCatalog | undefined): Set<string> {
  const byKey = permissionMap(catalog);
  const pending = [...granted];
  if (pending.includes('*')) return new Set(byKey.keys());
  const expanded = new Set<string>();
  while (pending.length > 0) {
    const key = pending.pop()!;
    if (expanded.has(key)) continue;
    const definition = byKey.get(key);
    if (!definition) continue;
    expanded.add(key);
    pending.push(...definition.implies);
  }
  return expanded;
}

/** Characters for generated passwords: no lookalikes (0/O, 1/l/I), nothing that needs quoting. */
export const PASSWORD_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789-_!@#%';

/** A strong, readable initial password, from the browser's cryptographic random source. */
export function generatePassword(length = 20, random: (values: Uint32Array) => Uint32Array = (values) => crypto.getRandomValues(values)): string {
  const values = random(new Uint32Array(length));
  return Array.from(values, (value) => PASSWORD_ALPHABET[value % PASSWORD_ALPHABET.length]).join('');
}

/** Which browser a user agent describes, roughly: enough to tell one's own sessions apart. */
export function describeAgent(agent: string | null): string {
  if (!agent) return 'Unknown browser';
  const browser = /Edg\//.test(agent)
    ? 'Edge'
    : /Chrome\//.test(agent)
      ? 'Chrome'
      : /Firefox\//.test(agent)
        ? 'Firefox'
        : /Safari\//.test(agent)
          ? 'Safari'
          : /curl\//.test(agent)
            ? 'curl'
            : agent.split(/[ /]/)[0] || 'Unknown browser';
  const platform = /Mac OS X|Macintosh/.test(agent)
    ? 'macOS'
    : /Windows/.test(agent)
      ? 'Windows'
      : /Android/.test(agent)
        ? 'Android'
        : /iPhone|iPad/.test(agent)
          ? 'iOS'
          : /Linux/.test(agent)
            ? 'Linux'
            : null;
  return platform ? `${browser} on ${platform}` : browser;
}

/** An audit entry's details as one readable line: changes as "before → after", lists joined. */
export function describeDetail(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—';
  if (Array.isArray(value)) return value.length === 0 ? 'none' : value.map(describeDetail).join(', ');
  if (typeof value === 'object') {
    const record = value as Record<string, unknown>;
    if ('from' in record || 'to' in record) return `${describeDetail(record.from)} → ${describeDetail(record.to)}`;
    return Object.entries(record)
      .map(([key, item]) => `${key.replace(/_/g, ' ')}: ${describeDetail(item)}`)
      .join('; ');
  }
  return String(value);
}

/** How a session was started, as a person would say it. */
export function describeMethod(method: string): string {
  if (method === 'password') return 'with a password';
  const provider = method.startsWith('sso:') ? method.slice(4) : method;
  const names: Record<string, string> = { entra: 'Microsoft', github: 'GitHub', google: 'Google', oidc: 'single sign-on' };
  return `through ${names[provider] ?? provider}`;
}
