/**
 * Data protection in the console (handbook 22): what a policy says, in people's words, and how two
 * policies differ. The control plane describes the kinds and masks (`GET /system/settings`), so the
 * console keeps no copy of them; the few words here only name the masks when nothing else does.
 */

import type { CpProtectionCatalog, CpProtectionKind, ProtectionMask, ProtectionPolicy } from '@shared/types';

/** How each mask is called when the control plane names none. */
export const MASK_NAMES: Record<ProtectionMask, string> = {
  hash: 'Hashed',
  partial: 'Partial',
  redact: 'Withheld',
};

/** One line under the control: what each mask does. */
export const MASK_LEGEND =
  'Hashed: equal values stay equal, so joins and counts work. Partial: readable, not joinable. Withheld: not stored.';

/** A policy's kinds and masks as a plain map, in the catalog's order. */
export type ProtectionChoice = Record<string, ProtectionMask>;

export function maskName(mask: string, catalog?: Pick<CpProtectionCatalog, 'masks'> | null): string {
  return catalog?.masks.find((item) => item.id === mask)?.label ?? MASK_NAMES[mask as ProtectionMask] ?? mask;
}

export function kindOf(catalog: Pick<CpProtectionCatalog, 'kinds'> | null | undefined, id: string): CpProtectionKind | undefined {
  return catalog?.kinds.find((kind) => kind.id === id);
}

export function kindLabel(catalog: Pick<CpProtectionCatalog, 'kinds'> | null | undefined, id: string): string {
  return kindOf(catalog, id)?.label ?? id.replace(/_/g, ' ');
}

/** The policy's choice, in the catalog's order. */
export function choiceOf(policy: ProtectionPolicy | ProtectionChoice | null | undefined, catalog?: Pick<CpProtectionCatalog, 'kinds'> | null): ProtectionChoice {
  const kinds: Record<string, string> = (policy && 'kinds' in policy && typeof policy.kinds === 'object' ? (policy as ProtectionPolicy).kinds : (policy as ProtectionChoice | null | undefined)) ?? {};
  const order = catalog?.kinds.map((kind) => kind.id) ?? Object.keys(kinds);
  const ordered: ProtectionChoice = {};
  for (const id of order) if (kinds[id]) ordered[id] = kinds[id] as ProtectionMask;
  for (const [id, mask] of Object.entries(kinds)) if (!ordered[id]) ordered[id] = mask as ProtectionMask;
  return ordered;
}

export function sameChoice(left: ProtectionChoice, right: ProtectionChoice): boolean {
  const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
  for (const key of keys) if (left[key] !== right[key]) return false;
  return true;
}

/** Whether a choice hashes anything, which needs the domain's protection key. */
export function hashes(choice: ProtectionChoice): boolean {
  return Object.values(choice).includes('hash');
}

/**
 * What the Data protection settings section says while the domain has no protection key, or null
 * once it has one: nothing can be hashed, so the control plane refuses a create that hashes - one
 * that keeps a default that hashes included.
 */
export function missingKeyNotice(catalog: Pick<CpProtectionCatalog, 'key_configured' | 'default'> | null | undefined): string | null {
  if (!catalog || catalog.key_configured) return null;
  const until = 'Nothing can be hashed here until protection-key is in the domain’s auth Secret and its apps have restarted (handbook 22 §22.9).';
  return hashes(catalog.default)
    ? `${until} Until then a new materialization that keeps this default is refused: choose partial or withheld for the hashed kinds, or add the key.`
    : `${until} Until then a materialization that chooses hashed is refused.`;
}

/** How a protected field's values are masked, in words: `Phone numbers, partial`. */
export function fieldProtectionText(protection: { kind: string; mask: string }, catalog?: CpProtectionCatalog | null): string {
  return `${kindLabel(catalog, protection.kind)}, ${maskName(protection.mask, catalog).toLowerCase()}`;
}

/** A choice in words: `Email addresses hashed · Passwords, tokens and keys withheld`, or `Nothing`. */
export function describeChoice(choice: ProtectionChoice, catalog?: CpProtectionCatalog | null): string {
  const entries = Object.entries(choice);
  if (entries.length === 0) return 'Nothing';
  return entries.map(([id, mask]) => `${kindLabel(catalog, id)} ${maskName(mask, catalog).toLowerCase()}`).join(' · ');
}

/** What changes from one choice to another, kind by kind, for a dialog to say before saving. */
export function choiceChanges(before: ProtectionChoice, after: ProtectionChoice, catalog?: CpProtectionCatalog | null): string[] {
  const changes: string[] = [];
  const ids = [...new Set([...Object.keys(before), ...Object.keys(after)])];
  for (const id of ids) {
    const from = before[id];
    const to = after[id];
    if (from === to) continue;
    const label = kindLabel(catalog, id);
    if (!from) changes.push(`${label}: now ${maskName(to!, catalog).toLowerCase()}`);
    else if (!to) changes.push(`${label}: no longer masked`);
    else changes.push(`${label}: ${maskName(from, catalog).toLowerCase()} → ${maskName(to, catalog).toLowerCase()}`);
  }
  return changes;
}
