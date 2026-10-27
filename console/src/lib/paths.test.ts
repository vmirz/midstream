import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { domainPath, sectionOf, sectionPath } from './paths.ts';

describe('domainPath', () => {
  it('puts a path under its domain, with or without its leading slash', () => {
    assert.equal(domainPath('domain_a'), '/d/domain_a');
    assert.equal(domainPath('domain_a', '/materializations'), '/d/domain_a/materializations');
    assert.equal(domainPath('domain_a', 'datasets/sales'), '/d/domain_a/datasets/sales');
  });

  it('encodes the domain as a path segment', () => {
    assert.equal(domainPath('a/b', '/x'), '/d/a%2Fb/x');
  });
});

describe('sectionOf', () => {
  it('reads the section whatever the scope', () => {
    assert.equal(sectionOf('/'), 'overview');
    assert.equal(sectionOf('/d/domain_a'), 'overview');
    assert.equal(sectionOf('/d/domain_a/'), 'overview');
    assert.equal(sectionOf('/materializations'), 'materializations');
    assert.equal(sectionOf('/d/domain_b/materializations/orders/data'), 'materializations');
    assert.equal(sectionOf('/d/domain_b/datasets/sales'), 'datasets');
    assert.equal(sectionOf('/search'), 'search');
    assert.equal(sectionOf('/settings/account'), 'settings');
    assert.equal(sectionOf('/d/domain_a/settings/users'), 'settings');
  });
});

describe('sectionPath', () => {
  it('keeps the section when the scope changes', () => {
    assert.equal(sectionPath('overview', null), '/');
    assert.equal(sectionPath('overview', 'domain_a'), '/d/domain_a');
    assert.equal(sectionPath('datasets', null), '/datasets');
    assert.equal(sectionPath('datasets', 'domain_b'), '/d/domain_b/datasets');
    assert.equal(sectionPath('settings', 'domain_b'), '/d/domain_b/settings');
  });

  it('keeps search the console’s own, whatever the scope', () => {
    assert.equal(sectionPath('search', 'domain_a'), '/search');
    assert.equal(sectionPath('search', null), '/search');
  });
});
