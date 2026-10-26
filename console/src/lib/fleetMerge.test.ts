import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { ConsoleFleet, DomainFleet, FleetSnapshot, FleetTotals, MaterializationSummary, ThroughputSample } from '@shared/types';

import { mergeFleet, mergeSeries } from './fleetMerge.ts';

function totals(overrides: Partial<FleetTotals> = {}): FleetTotals {
  return {
    materializations: 1,
    running: 1,
    stopped: 0,
    failing: 0,
    pending: 0,
    deleted: 0,
    rowsCommitted: 100,
    tableBytes: 1_000,
    tableFiles: 2,
    rowsWritten: 100,
    bytesWritten: 1_000,
    filesWritten: 2,
    recordsSkipped: 0,
    rowsPerSecond: 10,
    bytesPerSecond: 100,
    ...overrides,
  };
}

function item(id: string, updatedAt: string): MaterializationSummary {
  return { materialization_id: id, updated_at: updatedAt } as MaterializationSummary;
}

function sample(t: number, rows: number, rowsPerSecond: number | null = rows): ThroughputSample {
  return { t, rows, bytes: rows * 10, files: 1, skipped: 0, rowsPerSecond, bytesPerSecond: rowsPerSecond === null ? null : rowsPerSecond * 10, lag: 0 };
}

function snapshot(overrides: Partial<FleetSnapshot> = {}): FleetSnapshot {
  return { generatedAt: '2026-10-26T12:00:00.000Z', connected: true, controlPlaneError: null, totals: totals(), items: [], series: [], ...overrides };
}

function domain(id: string, overrides: Partial<DomainFleet> = {}): DomainFleet {
  return { domain: id, name: `Domain ${id.slice(-1).toUpperCase()}`, access: 'ok', snapshot: snapshot(), ...overrides };
}

function fleet(...domains: DomainFleet[]): ConsoleFleet {
  return { generatedAt: '2026-10-26T12:00:00.000Z', domains };
}

describe('mergeFleet', () => {
  it('merges every domain shown, newest change first, each item saying its domain', () => {
    const merged = mergeFleet(
      fleet(
        domain('domain_a', { snapshot: snapshot({ items: [item('orders', '2026-10-26T10:00:00Z')] }) }),
        domain('domain_b', { snapshot: snapshot({ items: [item('orders', '2026-10-26T11:00:00Z')] }) }),
      ),
      null,
    );
    assert.deepEqual(
      merged.items.map((entry) => `${entry.domain}/${entry.materialization_id}`),
      ['domain_b/orders', 'domain_a/orders'],
    );
    assert.equal(merged.items[0]!.domainName, 'Domain B');
    assert.equal(merged.totals.materializations, 2);
    assert.equal(merged.totals.rowsCommitted, 200);
    assert.equal(merged.totals.rowsPerSecond, 20);
  });

  it('shows one domain alone when asked', () => {
    const merged = mergeFleet(fleet(domain('domain_a'), domain('domain_b')), 'domain_b');
    assert.deepEqual(merged.domains.map((entry) => entry.domain), ['domain_b']);
    assert.equal(merged.totals.materializations, 1);
  });

  it('leaves out, and names, the domains the person may not see', () => {
    const merged = mergeFleet(
      fleet(domain('domain_a'), domain('domain_b', { access: 'sign_in_required', snapshot: null }), domain('domain_c', { access: 'forbidden', snapshot: null })),
      null,
    );
    assert.deepEqual(merged.domains.map((entry) => entry.domain), ['domain_a']);
    assert.deepEqual(merged.hidden.map((entry) => entry.access), ['sign_in_required', 'forbidden']);
    assert.equal(merged.totals.materializations, 1);
  });

  it('knows no rate summed over the domains while any domain’s is unknown', () => {
    const merged = mergeFleet(fleet(domain('domain_a'), domain('domain_b', { snapshot: snapshot({ totals: totals({ rowsPerSecond: null }) }) })), null);
    assert.equal(merged.totals.rowsPerSecond, null);
    assert.equal(merged.totals.bytesPerSecond, 200);
  });

  it('names the domain whose control plane cannot be read, and is not connected while it is not', () => {
    const merged = mergeFleet(
      fleet(domain('domain_a'), domain('domain_b', { snapshot: snapshot({ connected: false, controlPlaneError: 'connection refused' }) })),
      null,
    );
    assert.equal(merged.connected, false);
    assert.equal(merged.controlPlaneError, 'Domain B: connection refused');
    assert.equal(mergeFleet(fleet(domain('domain_b', { snapshot: snapshot({ controlPlaneError: 'connection refused' }) })), 'domain_b').controlPlaneError, 'connection refused');
  });
});

describe('mergeSeries', () => {
  it('sums points within the same few seconds', () => {
    const merged = mergeSeries([[sample(10_000, 5)], [sample(11_000, 7)]]);
    assert.equal(merged.length, 1);
    assert.equal(merged[0]!.t, 10_000);
    assert.equal(merged[0]!.rows, 12);
    assert.equal(merged[0]!.rowsPerSecond, 12);
  });

  it('carries a domain’s latest point into later buckets, so a later sample does not read as a dip', () => {
    const merged = mergeSeries([[sample(10_000, 5), sample(20_000, 6)], [sample(10_000, 7)]]);
    assert.deepEqual(merged.map((point) => point.rows), [12, 13]);
  });

  it('knows no rate while any domain’s is unknown', () => {
    const merged = mergeSeries([[sample(10_000, 5, null)], [sample(10_000, 7)]]);
    assert.equal(merged[0]!.rowsPerSecond, null);
  });
});
