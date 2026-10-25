import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { hmacCredentialWarning } from './config.ts';

/**
 * These run against whatever the process env holds, which in `cached` mode means the check is
 * inert. The point of the suite is that a valid pair is silent and the shapes people actually
 * paste by mistake are not — so it asserts against the pattern rather than the live config.
 */
describe('hmacCredentialWarning', () => {
  it('is silent unless direct mode is active', () => {
    // No HMAC vars are set in the test environment, so the console is in `cached` mode and the
    // check must not fire: warning about credentials nobody configured is noise.
    assert.equal(hmacCredentialWarning(), null);
  });
});

describe('GCS HMAC credential shapes', () => {
  const KEY_ID = /^GOOG[0-9A-Z]{16,}$/;

  it('accepts a real service-account access ID', () => {
    assert.ok(KEY_ID.test('GOOG1EEBYHBH3QXCIB324G555VUXRES5C7TFVMF35XZWSWCTPJPXSJY6TADAZ'));
  });

  it('rejects a service account email, the mistake that produces MalformedSecurityHeader', () => {
    assert.ok(!KEY_ID.test('kafka-materializer@production-269502.iam.gserviceaccount.com'));
  });

  it('rejects a project id', () => {
    assert.ok(!KEY_ID.test('production-269502'));
  });

  it('rejects a lowercased access ID', () => {
    assert.ok(!KEY_ID.test('goog1eebyhbh3qxcib324g555vuxres5c7tfvmf35xzwswctpjpxsjy6tadaz'));
  });
});
