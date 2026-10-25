import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CpSchemaField, FieldTimes, SchemaTimeline } from '@shared/types';

import { buildTimeline, dayOf, formatSpan, fromTheStart, stoppedArriving, versionDataSpans } from './timeline.ts';

// No zone given: dates are told in UTC whatever the zone the tests run in.
const EN_US = { locale: 'en-US' };

function field(fieldId: number, path: string[], overrides: Partial<CpSchemaField> = {}): CpSchemaField {
  return {
    field_id: fieldId,
    path,
    name: path.join('.').replace(/__v\d+/g, ''),
    type: 'string',
    required: false,
    kind: 'value',
    state: 'current',
    fork_version: 1,
    since_schema_id: 0,
    ...overrides,
  };
}

/** Values from `first` to `last`, each a day's span (a day-partitioned table's files). */
function seen(first: string, last: string = first): FieldTimes {
  return {
    first: { from: `${first}T00:00:00.000Z`, to: `${first}T23:00:00.000Z` },
    last: { from: `${last}T00:00:00.000Z`, to: `${last}T23:00:00.000Z` },
    files: 1,
    values: 1,
  };
}

function timeline(fields: Record<number, FieldTimes>, dataFrom = '2024-10-05', dataThrough = '2026-10-05'): SchemaTimeline {
  return {
    materializationId: 'mat-1',
    table: 'orders',
    available: true,
    reason: null,
    snapshotId: '1',
    granularity: 'day',
    dataFrom: `${dataFrom}T00:00:00.000Z`,
    dataThrough: `${dataThrough}T23:00:00.000Z`,
    fields: Object.fromEntries(Object.entries(fields).map(([id, times]) => [id, times])),
  };
}

// The load-test topic's history: v2 added `device` on 2025-04-15, v3 turned `customer.phone` into an
// object on 2025-11-03, v4 replaced `customer.email` by `customer.email_hash` on 2026-04-01.
const ORDER_ID = field(10, ['order_id']);
const CUSTOMER = field(11, ['customer'], { type: 'struct', kind: 'container' });
const EMAIL = field(12, ['customer', 'email']);
const PHONE = field(13, ['customer', 'phone'], { state: 'superseded' });
const DEVICE = field(14, ['device'], { since_schema_id: 0 });
const PHONE_V2 = field(15, ['customer', 'phone__v2'], { type: 'struct', kind: 'container', fork_version: 2, fork_of: 13, since_schema_id: 0 });
const PHONE_NUMBER = field(16, ['customer', 'phone__v2', 'number'], { since_schema_id: 0 });
const EMAIL_HASH = field(17, ['customer', 'email_hash'], { since_schema_id: 2 });
const GEO = field(18, ['ship_to_geo'], { since_schema_id: 1 });
const UNMAPPED = field(7, ['_unmapped'], { kind: 'metadata' });
const FIELDS = [ORDER_ID, CUSTOMER, EMAIL, PHONE, DEVICE, PHONE_V2, PHONE_NUMBER, EMAIL_HASH, GEO, UNMAPPED];

const HISTORY = timeline({
  10: seen('2024-10-05', '2026-10-05'),
  11: seen('2024-10-05', '2026-10-05'),
  12: seen('2024-10-05', '2026-04-03'),
  13: seen('2024-10-05', '2025-11-09'),
  14: seen('2025-04-15', '2026-10-05'),
  15: seen('2025-11-03', '2026-10-05'),
  16: seen('2025-11-03', '2026-10-05'),
  17: seen('2026-04-01', '2026-10-05'),
  18: seen('2025-06-01', '2026-03-30'),
  7: { ...seen('2025-08-01', '2025-08-02'), values: 42 },
});

describe('dates of a schema change, from the data', () => {
  it('writes a span as tightly as its bounds allow', () => {
    // Intl puts thin spaces around the dash.
    const span = (from: string, to: string) => formatSpan({ from, to }, EN_US).replace(/\s/g, ' ');
    assert.equal(span('2025-04-15T01:00:00Z', '2025-04-15T23:00:00Z'), 'Apr 15, 2025');
    assert.equal(span('2025-04-03T00:00:00Z', '2025-04-28T00:00:00Z'), 'Apr 3 – 28, 2025');
    assert.equal(span('2025-03-30T00:00:00Z', '2025-04-02T00:00:00Z'), 'Mar 30 – Apr 2, 2025');
    assert.equal(span('2024-12-30T00:00:00Z', '2025-01-02T00:00:00Z'), 'Dec 30, 2024 – Jan 2, 2025');
  });

  it('tells dates in UTC, the zone the table’s partitions are cut in, whatever the reader’s', () => {
    // A day partition's file, 00:36-16:22 UTC: one day in UTC, two in New York.
    const file = { from: '2025-04-15T00:36:00Z', to: '2025-04-15T16:22:00Z' };
    assert.equal(formatSpan(file, { locale: 'en-US' }), 'Apr 15, 2025');
    assert.equal(formatSpan(file, { locale: 'en-US', timeZone: 'America/New_York' }).replace(/\s/g, ' '), 'Apr 14 – 15, 2025');
    assert.equal(dayOf('2025-04-15T02:00:00Z'), '2025-04-15');
    assert.equal(dayOf('2025-04-15T02:00:00Z', { timeZone: 'America/New_York' }), '2025-04-14');
  });

  it('says a field stopped arriving only a day or more before the table’s last record', () => {
    assert.equal(stoppedArriving(HISTORY.fields['12'], HISTORY.dataThrough), true);
    assert.equal(stoppedArriving(HISTORY.fields['10'], HISTORY.dataThrough), false);
    assert.equal(stoppedArriving(undefined, HISTORY.dataThrough), false);
    assert.equal(fromTheStart(HISTORY.fields['12'], HISTORY, EN_US), true);
    assert.equal(fromTheStart(HISTORY.fields['14'], HISTORY, EN_US), false);
  });

  it('dates a version by the earliest data among the fields it introduced', () => {
    const spans = versionDataSpans(FIELDS, HISTORY.fields);
    // Schema 0 was sampled at both ends of the topic: its fields span its whole history.
    assert.equal(spans.get(0)?.from, '2024-10-05T00:00:00.000Z');
    // Schema 1, met during the backfill after schema 2's shapes were sampled, is older in the data.
    assert.equal(spans.get(1)?.from, '2025-06-01T00:00:00.000Z');
    assert.equal(spans.get(2)?.from, '2026-04-01T00:00:00.000Z');
    assert.equal(spans.has(3), false);
  });

  it('builds the history by day, newest first, whatever version holds each change', () => {
    const groups = buildTimeline(FIELDS, HISTORY, EN_US);
    const summary = groups.map((group) => [group.day, group.initial, group.entries.map((entry) => `${entry.kind} ${entry.field.name}`)]);
    assert.deepEqual(summary, [
      ['2026-04-03', false, ['stopped customer.email']],
      ['2026-04-01', false, ['added customer.email_hash']],
      ['2026-03-30', false, ['stopped ship_to_geo']],
      // The struct a fork made is one change: its keys are folded into it.
      ['2025-11-03', false, ['forked customer.phone']],
      ['2025-08-01', false, ['unmapped _unmapped']],
      ['2025-06-01', false, ['added ship_to_geo']],
      ['2025-04-15', false, ['added device']],
      // The first records: an object and its keys are one entry.
      ['2024-10-05', true, ['added customer', 'added order_id']],
    ]);
    const fork = groups[3]?.entries[0];
    assert.equal(fork?.previous?.field.field_id, 13);
    assert.equal(fork?.previous?.last?.from, '2025-11-09T00:00:00.000Z');
    assert.equal(groups[4]?.entries[0]?.values, 42);
  });

  it('dates a group by the span its entries cover: a month table’s file, not its first day', () => {
    const month = (from: string, to: string): FieldTimes => ({ first: { from, to }, last: { from, to }, files: 1, values: 1 });
    const april = timeline({
      10: seen('2025-01-01', '2025-04-30'),
      14: month('2025-04-01T00:13:00.000Z', '2025-04-30T21:02:00.000Z'),
      18: month('2025-04-01T02:40:00.000Z', '2025-04-29T23:10:00.000Z'),
    }, '2025-01-01', '2025-04-30');
    const groups = buildTimeline([ORDER_ID, DEVICE, GEO], april, EN_US);
    assert.equal(groups.length, 2);
    const [group] = groups;
    assert.equal(group?.day, '2025-04-01');
    assert.deepEqual(group?.span, { from: '2025-04-01T00:13:00.000Z', to: '2025-04-30T21:02:00.000Z' });
    assert.equal(formatSpan(group!.span, EN_US).replace(/\s/g, ' '), 'Apr 1 – 30, 2025');
    assert.deepEqual(group?.entries.map((entry) => entry.field.name), ['device', 'ship_to_geo']);
  });

  it('has nothing to say of a table that holds no values', () => {
    assert.deepEqual(buildTimeline(FIELDS, timeline({})), []);
  });
});
