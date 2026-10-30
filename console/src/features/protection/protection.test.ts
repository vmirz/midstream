import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CpProtectionCatalog } from '@shared/types';

import {
  choiceChanges,
  choiceOf,
  describeChoice,
  fieldProtectionText,
  hashes,
  kindLabel,
  maskName,
  missingKeyNotice,
  plaintextTotals,
  reprotectCounts,
  sameChoice,
} from './protection.ts';

const CATALOG: CpProtectionCatalog = {
  kinds: [
    {
      id: 'email',
      label: 'Email addresses',
      description: 'emails',
      default_mask: 'hash',
      masks: ['hash', 'partial', 'redact'],
      partial_example: { value: 'jane@example.com', partial: '****@example.com' },
      examples: ['email'],
    },
    {
      id: 'phone',
      label: 'Phone numbers',
      description: 'phones',
      default_mask: 'hash',
      masks: ['hash', 'partial', 'redact'],
      partial_example: { value: '540-948-9848', partial: '****9848' },
      examples: ['phone'],
    },
    {
      id: 'secret',
      label: 'Passwords, tokens and keys',
      description: 'secrets',
      default_mask: 'redact',
      masks: ['partial', 'redact'],
      partial_example: { value: 'hunter2', partial: '****' },
      examples: ['password'],
    },
  ],
  masks: [
    { id: 'hash', label: 'Hashed', description: '' },
    { id: 'partial', label: 'Partial', description: '' },
    { id: 'redact', label: 'Withheld', description: '' },
  ],
  default: { email: 'hash', phone: 'hash', secret: 'redact' },
  key_configured: true,
  key_id: 'abcd1234',
};

describe('data protection in words', () => {
  it('names a protected field\'s kind and mask as the catalog does', () => {
    assert.equal(fieldProtectionText({ kind: 'phone', mask: 'partial' }, CATALOG), 'Phone numbers, partial');
    assert.equal(fieldProtectionText({ kind: 'national_id', mask: 'hash' }, null), 'national id, hashed');
  });

  it('warns of a missing protection key, and of a default that hashes without one', () => {
    assert.equal(missingKeyNotice(CATALOG), null);
    assert.equal(missingKeyNotice(null), null);
    const keyless = { ...CATALOG, key_configured: false, key_id: null };
    assert.match(missingKeyNotice(keyless) ?? '', /keeps this default is refused/);
    assert.match(missingKeyNotice({ ...keyless, default: { email: 'partial', secret: 'redact' } }) ?? '', /chooses hashed is refused/);
  });

  it('orders a policy as the catalog does, from a policy or a plain choice', () => {
    assert.deepEqual(Object.keys(choiceOf({ kinds: { secret: 'redact', email: 'hash' } }, CATALOG)), ['email', 'secret']);
    assert.deepEqual(choiceOf({ phone: 'partial' }, CATALOG), { phone: 'partial' });
    assert.deepEqual(choiceOf(null, CATALOG), {});
  });

  it('names kinds and masks from the catalog, and without one', () => {
    assert.equal(kindLabel(CATALOG, 'phone'), 'Phone numbers');
    assert.equal(kindLabel(null, 'national_id'), 'national id');
    assert.equal(maskName('redact', CATALOG), 'Withheld');
    assert.equal(maskName('partial'), 'Partial');
  });

  it('describes a policy and says nothing is protected by name', () => {
    assert.equal(describeChoice({ email: 'hash', secret: 'redact' }, CATALOG), 'Email addresses hashed · Passwords, tokens and keys withheld');
    assert.equal(describeChoice({}, CATALOG), 'Nothing');
  });

  it('compares choices whatever their order, and knows when one hashes', () => {
    assert.equal(sameChoice({ email: 'hash', phone: 'hash' }, { phone: 'hash', email: 'hash' }), true);
    assert.equal(sameChoice({ email: 'hash' }, { email: 'partial' }), false);
    assert.equal(sameChoice({ email: 'hash' }, {}), false);
    assert.equal(hashes({ phone: 'partial', secret: 'redact' }), false);
    assert.equal(hashes({ phone: 'hash' }), true);
  });

  it('says what an edit changes, kind by kind', () => {
    assert.deepEqual(choiceChanges({ email: 'hash', phone: 'hash' }, { phone: 'partial', secret: 'redact' }, CATALOG), [
      'Email addresses: no longer masked',
      'Phone numbers: hashed → partial',
      'Passwords, tokens and keys: now withheld',
    ]);
  });
});

describe('reprotect', () => {
  it('says what a run rewrote, masked and purged, leaving out what it did not do', () => {
    assert.deepEqual(reprotectCounts({ counts: { inputs: 1, outputs: 1, masked: 1200, withheld: 0, moved: 3, expired_snapshots: 4, deleted_files: 2, files_left: 0 } }), [
      '1 file rewritten',
      '1,200 values masked',
      '3 values moved masked to a text field',
      '4 older snapshots expired',
      '2 replaced files deleted',
    ]);
    assert.deepEqual(reprotectCounts({ counts: {} }), []);
  });

  it('sums the plain text of the tables counted, each blocked reason once', () => {
    const report = { revision: 2, counted_at: '2026-10-30T10:00:00Z', snapshot_id: '7', files: 2, rows: 40, bytes: 4096, blocked_files: 1, blocked: ['ip has no text field yet for its masked values; the writers add it with its next value'], unknown_files: 0 };
    assert.deepEqual(plaintextTotals([report, null, { ...report, files: 1, rows: 5, unknown_files: 3 }, undefined]), {
      counted: 2,
      files: 3,
      rows: 45,
      bytes: 8192,
      blockedFiles: 2,
      blocked: ['ip has no text field yet for its masked values; the writers add it with its next value'],
      unknownFiles: 3,
    });
  });
});
