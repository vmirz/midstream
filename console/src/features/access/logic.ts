/**
 * The access pages' logic that needs no React: permission implications, generated passwords, and
 * telling sessions apart. Kept apart from the components so it can be tested directly.
 */

import type { CpPermissionCatalog, CpPermissionDefinition } from '@shared/types';

import { humanize } from '../../lib/format.ts';

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

/** What an audit entry's action reads as. */
export const AUDIT_ACTION_LABELS: Record<string, string> = {
  'session.signed_in': 'Signed in',
  'session.sign_in_failed': 'Failed sign-in',
  'session.locked_out': 'Locked out',
  'session.signed_out': 'Signed out',
  'user.created': 'Added a user',
  'user.updated': 'Changed a user',
  'user.deactivated': 'Deactivated a user',
  'user.reactivated': 'Reactivated a user',
  'user.deleted': 'Deleted a user',
  'user.password_reset': 'Reset a password',
  'user.password_changed': 'Changed their password',
  'user.signed_out': 'Signed a user out everywhere',
  'user.session_ended': 'Ended a session',
  'user.unlocked': 'Unlocked a user',
  'user.bootstrapped': 'Made the first administrator',
  'user.bootstrap_restored': 'Restored the bootstrap administrator',
  'user.provisioned': 'Made an account at a first sign-in',
  'user.identity_linked': 'Linked a sign-in provider',
  'group.created': 'Created a group',
  'group.updated': 'Changed a group',
  'group.deleted': 'Deleted a group',
  'role.created': 'Created a role',
  'role.updated': 'Changed a role',
  'role.deleted': 'Deleted a role',
  'token.created': 'Made an access token',
  'token.revoked': 'Revoked an access token',
  'token.revocation_applied': 'Recorded a token revoked in another domain',
  'dataset.created': 'Created a dataset',
  'dataset.updated': 'Changed a dataset',
  'dataset.deleted': 'Deleted a dataset',
  'dataset.access_changed': 'Changed who may use a dataset',
  'directory.seeded': 'Set up the built-in groups',
};

/** What an audit entry's action reads as: its label, or for one this console does not know, its code in words. */
export function auditActionLabel(action: string): string {
  return AUDIT_ACTION_LABELS[action] ?? humanize(action.replace('.', ' '));
}
