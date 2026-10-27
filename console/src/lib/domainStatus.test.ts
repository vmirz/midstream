import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { domainStatusText, strayKeys } from './domainStatus.ts';

describe('strayKeys', () => {
  it('names no key while the domains agree, or fewer than two say', () => {
    assert.deepEqual([...strayKeys([{ federationKeyId: 'aaaa1111' }, { federationKeyId: 'aaaa1111' }])], []);
    assert.deepEqual([...strayKeys([{ federationKeyId: 'aaaa1111' }, { federationKeyId: null }])], []);
    assert.deepEqual([...strayKeys([])], []);
  });

  it('names the keys that are not the one most domains share', () => {
    const domains = [{ federationKeyId: 'aaaa1111' }, { federationKeyId: 'bbbb2222' }, { federationKeyId: 'aaaa1111' }];
    assert.deepEqual([...strayKeys(domains)], ['bbbb2222']);
  });

  it('on a tie, names every key but the first seen', () => {
    assert.deepEqual([...strayKeys([{ federationKeyId: 'aaaa1111' }, { federationKeyId: 'bbbb2222' }])], ['bbbb2222']);
  });
});

describe('domainStatusText', () => {
  it('prefers what the console said', () => {
    assert.equal(domainStatusText({ status: 'misconfigured', connected: false, statusMessage: 'says it is domain_b' }), 'says it is domain_b');
  });

  it('says each status in words otherwise', () => {
    assert.equal(domainStatusText({ status: 'ok', connected: true, statusMessage: null }), 'Live');
    assert.equal(domainStatusText({ status: 'ok', connected: false, statusMessage: null }), 'Reconnecting');
    assert.match(domainStatusText({ status: 'unreachable', connected: false, statusMessage: null }), /cannot be reached/);
  });
});
