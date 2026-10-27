import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CONSOLE_SECTIONS, DOMAIN_SECTIONS, findSection, visibleSections } from './sections.ts';

describe('the settings pages', () => {
  it('list each section once per page', () => {
    for (const sections of [CONSOLE_SECTIONS, DOMAIN_SECTIONS]) {
      const ids = sections.map((section) => section.id);
      assert.equal(new Set(ids).size, ids.length);
    }
  });

  it('keep the directory in the domains and the account, the registry and every domain’s history on the console’s', () => {
    for (const id of ['users', 'groups', 'roles', 'audit']) {
      assert.ok(findSection(id, DOMAIN_SECTIONS), id);
      assert.equal(findSection(id, CONSOLE_SECTIONS), undefined, id);
    }
    for (const id of ['account', 'domains']) {
      assert.ok(findSection(id, CONSOLE_SECTIONS), id);
      assert.equal(findSection(id, DOMAIN_SECTIONS), undefined, id);
    }
    assert.equal(findSection('history', CONSOLE_SECTIONS)?.heldIn, 'any');
  });
});

describe('visibleSections', () => {
  it('shows the account and the domains to anyone signed in, and nothing else without a permission', () => {
    const shown = visibleSections(CONSOLE_SECTIONS, true, () => false).map((section) => section.id);
    assert.deepEqual(shown, ['account', 'domains']);
    assert.deepEqual(visibleSections(CONSOLE_SECTIONS, false, () => false), []);
  });

  it('checks the console’s settings in the settings home, and its history in any domain', () => {
    const inAnyDomainOnly = visibleSections(CONSOLE_SECTIONS, true, (permission, heldIn) => permission === 'settings.view' && heldIn === 'any');
    assert.deepEqual(
      inAnyDomainOnly.map((section) => section.id),
      ['account', 'domains', 'history'],
    );
    const inHome = visibleSections(CONSOLE_SECTIONS, true, (permission) => permission === 'settings.view');
    assert.ok(inHome.some((section) => section.id === 'live'));
    assert.ok(inHome.some((section) => section.id === 'deployment'));
  });

  it('shows a domain’s directory to whoever may see it there', () => {
    const shown = visibleSections(DOMAIN_SECTIONS, true, (permission) => permission === 'users.view').map((section) => section.id);
    assert.deepEqual(shown, ['users']);
  });
});
