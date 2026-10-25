import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DATASET_PERMISSIONS, can, canAnywhere, isDatasetPermission, permissionsOn, type Holder } from './access.ts';

const SALES = 'ds-000000000001';
const FINANCE = 'ds-000000000002';

/** A person who may see materializations everywhere, owns sales and reads nothing else. */
const owner: Holder = {
  permissions: ['materializations.view', 'materializations.view_details', 'datasets.view'],
  all_permissions: false,
  dataset_permissions: { [SALES]: ['dataset.view_schema', 'data.query', 'data.export', 'catalog.read', 'dataset.write', 'dataset.manage'] },
  dataset_limit: null,
};

describe('dataset permissions', () => {
  it('are told apart from the deployment ones', () => {
    assert.ok(isDatasetPermission('data.query'));
    assert.ok(isDatasetPermission('catalog.write'));
    assert.ok(!isDatasetPermission('datasets.view'), 'listing datasets is held across the deployment');
    assert.ok(!isDatasetPermission('materializations.create'));
  });

  it('hold on the datasets they are granted on, and nowhere else', () => {
    assert.ok(can(owner, 'data.query', SALES));
    assert.ok(!can(owner, 'data.query', FINANCE));
    assert.ok(!can(owner, 'data.query'), 'not across the deployment');
    assert.ok(canAnywhere(owner, 'data.query'));
    assert.ok(!canAnywhere(owner, 'catalog.write'));
    assert.ok(can(owner, 'materializations.view_details', FINANCE), 'a deployment permission ignores the dataset');
  });

  it('held across the deployment, hold on every dataset', () => {
    const reader: Holder = { permissions: ['dataset.view_schema', 'data.query'], all_permissions: false };
    assert.ok(can(reader, 'data.query', FINANCE));
    assert.ok(can(reader, 'data.query'));
    assert.deepEqual([...permissionsOn(reader, SALES)].sort(), ['data.query', 'dataset.view_schema']);
  });

  it('are all held by an administrator', () => {
    const admin: Holder = { permissions: [], all_permissions: true };
    assert.deepEqual([...permissionsOn(admin, FINANCE)].sort(), [...DATASET_PERMISSIONS].sort());
    assert.ok(can(admin, 'dataset.manage', FINANCE));
  });

  it('of a token limited to datasets hold on those alone', () => {
    const token: Holder = {
      permissions: ['catalog.read', 'dataset.view_schema'],
      all_permissions: false,
      dataset_permissions: {},
      dataset_limit: [SALES],
    };
    assert.ok(can(token, 'catalog.read', SALES));
    assert.ok(!can(token, 'catalog.read', FINANCE));
    assert.ok(!can(token, 'catalog.read'), 'a limited token holds no dataset permission everywhere');
    assert.ok(canAnywhere(token, 'catalog.read'));
  });

  it('are not held by someone who must change their password', () => {
    assert.ok(!can({ ...owner, password_change_required: true }, 'data.query', SALES));
    assert.ok(!canAnywhere({ ...owner, password_change_required: true }, 'data.query'));
  });

  it('hold nothing extra for a principal from before datasets', () => {
    const old: Holder = { permissions: ['data.query'], all_permissions: false };
    assert.ok(can(old, 'data.query', SALES));
    assert.ok(canAnywhere(old, 'data.query'));
  });
});
