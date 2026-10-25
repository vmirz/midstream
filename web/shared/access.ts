/**
 * What a principal may do, across the deployment and on each dataset.
 *
 * A permission has a scope. A deployment permission (`materializations.create`, `datasets.view`, …)
 * is held across the deployment or not at all. A dataset permission (`data.query`,
 * `dataset.view_schema`, …) is held on a dataset: held across the deployment, it holds on every
 * dataset; granted on one dataset, it holds there only. An access token limited to some datasets
 * holds its dataset permissions on those alone.
 *
 * The control plane decides (`identity/principals.py`); this mirrors it so the console can check a
 * permission on a dataset without asking, from the principal `/auth/session` reports. Dependency-free:
 * imported by the server and the browser alike.
 */

import type { CpPrincipal } from './types.ts';

/** The permissions that are held on a dataset (the control plane's `DATASET_PERMISSION_KEYS`). */
export const DATASET_PERMISSIONS = [
  'dataset.view_schema',
  'data.query',
  'data.export',
  'catalog.read',
  'catalog.write',
  'dataset.write',
  'dataset.manage',
] as const;

export type DatasetPermission = (typeof DATASET_PERMISSIONS)[number];

const DATASET_PERMISSION_SET: ReadonlySet<string> = new Set(DATASET_PERMISSIONS);

/** What `can` needs of a principal. */
export type Holder = Pick<CpPrincipal, 'permissions' | 'all_permissions'> &
  Partial<Pick<CpPrincipal, 'dataset_permissions' | 'dataset_limit' | 'password_change_required'>>;

/** Whether a permission is held on a dataset rather than across the deployment. */
export function isDatasetPermission(permission: string): boolean {
  return DATASET_PERMISSION_SET.has(permission);
}

/** Every dataset permission held on one dataset, by its id. */
export function permissionsOn(holder: Holder, datasetId: string): Set<string> {
  const limit = holder.dataset_limit;
  if (limit && !limit.includes(datasetId)) return new Set();
  if (holder.all_permissions) return new Set(DATASET_PERMISSIONS);
  const held = new Set(holder.permissions.filter(isDatasetPermission));
  for (const permission of holder.dataset_permissions?.[datasetId] ?? []) held.add(permission);
  return held;
}

/**
 * Whether a permission is held: on `datasetId` when one is named and the permission is about
 * datasets; across the deployment otherwise. Someone who must change their password holds nothing.
 */
export function can(holder: Holder, permission: string, datasetId?: string | null): boolean {
  if (holder.password_change_required) return false;
  if (datasetId && isDatasetPermission(permission)) return permissionsOn(holder, datasetId).has(permission);
  // A token limited to some datasets holds no dataset permission on every dataset.
  if (isDatasetPermission(permission) && holder.dataset_limit) return false;
  return holder.all_permissions || holder.permissions.includes(permission);
}

/** Whether a permission is held across the deployment, or on at least one dataset. */
export function canAnywhere(holder: Holder, permission: string): boolean {
  if (can(holder, permission)) return true;
  if (holder.password_change_required || !isDatasetPermission(permission)) return false;
  const candidates = holder.dataset_limit ?? Object.keys(holder.dataset_permissions ?? {});
  return candidates.some((datasetId) => permissionsOn(holder, datasetId).has(permission));
}
