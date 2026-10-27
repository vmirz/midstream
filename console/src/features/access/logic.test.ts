import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CpPermissionCatalog } from '@shared/types';

import { AUDIT_ACTION_LABELS, PASSWORD_ALPHABET, auditActionLabel, describeAgent, describeDetail, describeMethod, expandPermissions, generatePassword } from './logic.ts';

const catalog: CpPermissionCatalog = {
  categories: [{ id: 'data', label: 'Data' }],
  permissions: [
    { key: 'materializations.view', category: 'materializations', label: 'View', description: '', implies: [], danger: false, scope: 'deployment' },
    {
      key: 'materializations.view_details',
      category: 'materializations',
      label: 'Details',
      description: '',
      implies: ['materializations.view'],
      danger: false,
      scope: 'deployment',
    },
    { key: 'dataset.view_schema', category: 'datasets', label: 'Schemas', description: '', implies: [], danger: false, scope: 'dataset' },
    { key: 'data.query', category: 'data', label: 'Query', description: '', implies: ['dataset.view_schema'], danger: false, scope: 'dataset' },
    { key: 'data.export', category: 'data', label: 'Export', description: '', implies: ['data.query'], danger: false, scope: 'dataset' },
  ],
};

describe('permission implications', () => {
  it('follow chains, as the control plane resolves them, each within its scope', () => {
    assert.deepEqual([...expandPermissions(['data.export'], catalog)].sort(), ['data.export', 'data.query', 'dataset.view_schema']);
    assert.deepEqual([...expandPermissions(['materializations.view_details'], catalog)].sort(), [
      'materializations.view',
      'materializations.view_details',
    ]);
  });

  it('read * as every permission and drop keys the catalog does not know', () => {
    assert.equal(expandPermissions(['*'], catalog).size, 5);
    assert.deepEqual([...expandPermissions(['nope', 'materializations.view'], catalog)], ['materializations.view']);
    assert.equal(expandPermissions(['data.query'], undefined).size, 0);
  });
});

describe('generated passwords', () => {
  it('are as long as asked, from an alphabet without lookalikes', () => {
    const password = generatePassword(20);
    assert.equal(password.length, 20);
    assert.ok([...password].every((character) => PASSWORD_ALPHABET.includes(character)));
    assert.doesNotMatch(PASSWORD_ALPHABET, /[0O1lI]/);
  });

  it('take every character from the random source', () => {
    const fixed = (values: Uint32Array): Uint32Array => values.fill(0);
    assert.equal(generatePassword(4, fixed), PASSWORD_ALPHABET[0]!.repeat(4));
  });
});

describe('describing sessions and audit details', () => {
  it('names the browser and the platform', () => {
    assert.equal(
      describeAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36'),
      'Chrome on macOS',
    );
    assert.equal(describeAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0'), 'Firefox on Windows');
    assert.equal(describeAgent('curl/8.7.1'), 'curl');
    assert.equal(describeAgent(null), 'Unknown browser');
  });

  it('reads changes as before and after', () => {
    assert.equal(describeDetail({ from: 'Ops', to: 'Operations' }), 'Ops → Operations');
    assert.equal(describeDetail(['Viewer', 'Editor']), 'Viewer, Editor');
    assert.equal(describeDetail({ added: ['Editor'], removed: [] }), 'added: Editor; removed: none');
    assert.equal(describeDetail(null), '—');
  });
});

describe('describeMethod', () => {
  it('says how a session was started', () => {
    assert.equal(describeMethod('password'), 'with a password');
    assert.equal(describeMethod('sso:entra'), 'through Microsoft');
    assert.equal(describeMethod('sso:github'), 'through GitHub');
    assert.equal(describeMethod('sso:oidc'), 'through single sign-on');
    assert.equal(describeMethod('sso:other'), 'through other');
  });
});

describe('audit actions', () => {
  // Every action the control plane audits (handbook 18): each reads as words on the audit page.
  const AUDITED = [
    'session.signed_in', 'session.sign_in_failed', 'session.locked_out', 'session.signed_out',
    'user.created', 'user.updated', 'user.deactivated', 'user.reactivated', 'user.deleted', 'user.password_reset',
    'user.password_changed', 'user.signed_out', 'user.session_ended', 'user.unlocked', 'user.bootstrapped',
    'user.bootstrap_restored', 'user.provisioned', 'user.identity_linked',
    'group.created', 'group.updated', 'group.deleted', 'role.created', 'role.updated', 'role.deleted',
    'dataset.created', 'dataset.updated', 'dataset.deleted', 'dataset.access_changed',
    'token.created', 'token.revoked', 'token.revocation_applied', 'directory.seeded',
  ];

  it('label every action the control plane audits', () => {
    assert.deepEqual(AUDITED.filter((action) => !(action in AUDIT_ACTION_LABELS)), []);
    assert.equal(auditActionLabel('token.revocation_applied'), 'Recorded a token revoked in another domain');
    assert.equal(auditActionLabel('dataset.access_changed'), 'Changed who may use a dataset');
  });

  it('read an action this console does not know as words, never as its code', () => {
    assert.equal(auditActionLabel('pipeline.started_again'), 'Pipeline started again');
  });
});
