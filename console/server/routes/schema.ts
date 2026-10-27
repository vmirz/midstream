import type { FastifyInstance, FastifyRequest } from 'fastify';

import { SERVICE } from '../controlPlane/client.ts';
import type { Domain } from '../domains/domain.ts';
import { authOf, domainOf, permit, requireOnDatasets } from '../auth/guard.ts';
import { dataAccessMode } from '../config.ts';
import { unreadableTable } from '../data/catalog.ts';
import { badRequest } from '../errors.ts';
import { partitionGranularity } from '../iceberg/metadata.ts';
import { tablePresence, type FieldPresence, type Span } from '../iceberg/presence.ts';
import { loadTable } from '../iceberg/table.ts';
import { tableSpec } from '../live/store.ts';
import { logger } from '../logger.ts';
import { diffSchemas } from '../schema/diff.ts';
import { materializationId } from './materializations.ts';
import type { CpSchema, CpSchemaVersions, FieldTimes, PartitionGranularity, SchemaTimeline, TimeSpan } from '../../shared/types.ts';

function parseSchemaId(value: unknown, field: string): number | null {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed < 0 || String(parsed) !== String(value).trim()) {
    throw badRequest(`${field} must be a schema id: a whole number from 0`, { field });
  }
  return parsed;
}

/**
 * The materialization and the name of the table a request is about, as the materialization spells
 * it - once the caller is known to be allowed to see the schemas of the table's dataset.
 */
async function tableOf(request: FastifyRequest): Promise<{ id: string; table: string; summary: TableSummary | null }> {
  const id = materializationId(request);
  const requested = (request.params as { table?: string }).table ?? '';
  const detail = await domainOf(request).live.getDetail(id);
  const spec = tableSpec(detail, requested);
  requireOnDatasets(authOf(request), 'dataset.view_schema', [{ id: spec.dataset_id, name: spec.dataset }]);
  const summary = detail.tables.find((candidate) => candidate.name === spec.name);
  return {
    id,
    table: spec.name,
    summary: summary ? { location: summary.location ?? null, granularity: summary.partition_granularity ?? 'day' } : null,
  };
}

interface TableSummary {
  location: string | null;
  granularity: PartitionGranularity;
}

/**
 * The Schema tab's reads, for one of a materialization's tables.
 *
 * The control plane keeps every schema each table has had, as the runtime committed it, so every
 * replica and every console sees the same history; the console only adds the diff between two. The
 * console reads them with its own credential, which sees every dataset's schemas, so each read
 * checks first that the caller may see the schemas of the table's dataset.
 */
export async function registerSchemaRoutes(app: FastifyInstance): Promise<void> {
  app.get('/materializations/:materializationId/tables/:table/schema', { preHandler: permit('materializations.view_details') }, async (request) => {
    const { id, table } = await tableOf(request);
    return domainOf(request).live.getSchema(id, table);
  });

  app.get(
    '/materializations/:materializationId/tables/:table/schema/versions',
    { preHandler: permit('materializations.view_details') },
    async (request) => {
      const { id, table } = await tableOf(request);
      return domainOf(request).controlPlane.schemaVersions(SERVICE, id, table);
    },
  );

  app.get(
    '/materializations/:materializationId/tables/:table/schema/versions/:schemaId',
    { preHandler: permit('materializations.view_details') },
    async (request) => {
      const { id, table } = await tableOf(request);
      const schemaId = parseSchemaId((request.params as { schemaId?: string }).schemaId, 'schema_id');
      if (schemaId === null) throw badRequest('schema_id is required', { field: 'schema_id' });
      return domainOf(request).controlPlane.schemaVersion(SERVICE, id, table, schemaId);
    },
  );

  /**
   * When each field appeared in the table's records and when it last did, by their Kafka timestamps,
   * from the table's manifests: what dates a schema change by when it happened. Its records, not the
   * order the runtime read them in, decide - a backfill reads partitions at their own pace, and schema
   * 0 already holds what discovery sampled from the topic's newest records beside its oldest.
   */
  app.get(
    '/materializations/:materializationId/tables/:table/schema/timeline',
    { preHandler: permit('materializations.view_details') },
    async (request): Promise<SchemaTimeline> => {
      const { id, table, summary } = await tableOf(request);
      return schemaTimeline(id, table, summary);
    },
  );

  /** `from` defaults to the version before `to`, and `to` to the current schema; schema 0 diffs against nothing. */
  app.get('/materializations/:materializationId/tables/:table/schema/diff', { preHandler: permit('materializations.view_details') }, async (request) => {
    const { id, table } = await tableOf(request);
    const query = request.query as { from?: string; to?: string };
    const current = await domainOf(request).live.getSchema(id, table);
    const toId = parseSchemaId(query.to, 'to') ?? current.schema_id;
    let fromId = parseSchemaId(query.from, 'from');
    if (fromId === null && toId !== null && query.from === undefined) {
      const versions = await schemaVersions(domainOf(request), id, table);
      fromId = versions?.versions.map((entry) => entry.schema_id).filter((entry) => entry < toId).sort((a, b) => b - a)[0] ?? null;
    }
    const after = toId === null || toId === current.schema_id ? current : await domainOf(request).controlPlane.schemaVersion(SERVICE, id, table, toId);
    const before = fromId === null ? null : await schemaAt(domainOf(request), id, table, fromId, current);
    return diffSchemas(id, before, after);
  });
}

async function schemaVersions(domain: Domain, id: string, table: string): Promise<CpSchemaVersions | null> {
  try {
    return await domain.controlPlane.schemaVersions(SERVICE, id, table);
  } catch (cause) {
    logger.debug({ err: cause, materializationId: id, table }, 'schema history unavailable');
    return null;
  }
}

async function schemaAt(domain: Domain, id: string, table: string, schemaId: number, current: CpSchema): Promise<CpSchema> {
  return schemaId === current.schema_id ? current : domain.controlPlane.schemaVersion(SERVICE, id, table, schemaId);
}

async function schemaTimeline(id: string, table: string, summary: TableSummary | null): Promise<SchemaTimeline> {
  const unavailable = (reason: string, granularity = summary?.granularity ?? 'day'): SchemaTimeline => ({
    materializationId: id,
    table,
    available: false,
    reason,
    snapshotId: null,
    granularity,
    dataFrom: null,
    dataThrough: null,
    fields: {},
  });
  const location = summary?.location ?? null;
  if (!location) return unavailable('the table has not been created yet');
  if (dataAccessMode() === 'disabled') return unavailable('data access is turned off for this console (Settings › Data explorer)');
  try {
    const loaded = await loadTable(location);
    if (!loaded) return unavailable('the table has not been created yet');
    const granularity = partitionGranularity(loaded.metadata);
    const presence = await tablePresence(loaded.metadata);
    if (presence.snapshotId === null) return unavailable('nothing has been committed to the table yet', granularity);
    return {
      materializationId: id,
      table,
      available: true,
      reason: null,
      snapshotId: presence.snapshotId,
      granularity,
      dataFrom: isoOf(presence.dataFrom),
      dataThrough: isoOf(presence.dataThrough),
      fields: Object.fromEntries([...presence.fields].map(([fieldId, field]) => [String(fieldId), timesOf(field)])),
    };
  } catch (cause) {
    logger.warn({ err: cause, materializationId: id, table }, 'table could not be read');
    return unavailable(unreadableTable(location, cause).message);
  }
}

function timesOf(field: FieldPresence): FieldTimes {
  return { first: spanOf(field.first), last: spanOf(field.last), files: field.files, values: field.values };
}

function spanOf(span: Span | null): TimeSpan | null {
  return span ? { from: new Date(span.from).toISOString(), to: new Date(span.to).toISOString() } : null;
}

function isoOf(value: number | null): string | null {
  return value === null ? null : new Date(value).toISOString();
}
