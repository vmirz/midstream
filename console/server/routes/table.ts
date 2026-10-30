import type { FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';

import { SERVICE } from '../controlPlane/client.ts';
import { consoleBaseUrl } from '../consoleUrl.ts';
import type { Domain } from '../domains/domain.ts';
import { authOf, callerOf, domainOf, permit, permitAnywhere } from '../auth/guard.ts';
import { can } from '../auth/principal.ts';
import { dataAccessMode } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { tableSpec } from '../live/store.ts';
import { logger } from '../logger.ts';
import { unreadableTable } from '../data/catalog.ts';
import { compactionDue, type CompactionThresholds } from '../iceberg/compaction.ts';
import { currentSchema, partitionGranularity, schemaById, type TableMetadata } from '../iceberg/metadata.ts';
import { FORK_MAP_PROPERTY, PROTECTION_PROPERTY } from '../iceberg/schema.ts';
import { snapshotCatalog, type DataObject, type SnapshotCatalog } from '../iceberg/snapshot.ts';
import { loadTable } from '../iceberg/table.ts';
import { materializationId, parseBody } from './materializations.ts';
import {
  MAINTENANCE_JOBS,
  type CatalogConnection,
  type CpSettingsScope,
  type CpTable,
  type TableMetadataView,
  type TablePartitionRow,
  type TableSnapshotRow,
  type TableView,
} from '../../shared/types.ts';

/** A job, on one table or, without `table`, on every table in turn; `purge` is a reprotect's. */
export const maintenanceSchema = z
  .strictObject({
    job: z.enum(MAINTENANCE_JOBS),
    table: z.string().min(1).max(128).nullish(),
    purge: z.boolean().optional(),
  })
  .refine((body) => body.purge === undefined || body.job === 'reprotect', { message: 'purge is an option of the reprotect job', path: ['purge'] });

const SNAPSHOT_ROWS = 200;
const DEFAULT_SMALL_FILE_BYTES = 64 * 1024 * 1024;
const DEFAULT_MIN_INPUT_FILES = 8;
const DEFAULT_HOT_PARTITION_DELAY_MINUTES = 60;
const SETTINGS_TTL_MS = 60_000;

interface Defaults extends CompactionThresholds {
  catalogEnabled: boolean;
  readAt: number;
}

/** Each domain's, by id. */
const defaults = new Map<string, Defaults>();

/**
 * What the Table tab needs from a domain's settings: its runtimes' compaction thresholds, and whether
 * its catalog is on. Read with the domain's service token, which may read settings, and kept a
 * minute; a control plane that cannot say falls back to the shipped defaults.
 */
async function tableDefaults(domain: Domain): Promise<Defaults> {
  const kept = defaults.get(domain.id);
  if (kept && Date.now() - kept.readAt < SETTINGS_TTL_MS) return kept;
  const [runtime, controlPlaneScope] = await Promise.all([
    domain.controlPlane.settingsScope(SERVICE, 'runtime').catch((cause: unknown) => logUnavailable(cause, 'runtime')),
    domain.controlPlane.settingsScope(SERVICE, 'control-plane').catch((cause: unknown) => logUnavailable(cause, 'control-plane')),
  ]);
  const setting = (scope: CpSettingsScope | null, key: string): unknown => scope?.settings?.find((item) => item.key === key)?.value;
  const read: Defaults = {
    smallFileBytes: numberOr(setting(runtime, 'compaction_small_file_bytes'), DEFAULT_SMALL_FILE_BYTES),
    minInputFiles: numberOr(setting(runtime, 'compaction_min_input_files'), DEFAULT_MIN_INPUT_FILES),
    hotPartitionDelayMinutes: numberOr(setting(runtime, 'compaction_hot_partition_delay_minutes'), DEFAULT_HOT_PARTITION_DELAY_MINUTES),
    catalogEnabled: setting(controlPlaneScope, 'catalog_api_enabled') !== false,
    readAt: Date.now(),
  };
  defaults.set(domain.id, read);
  return read;
}

/**
 * Where engines reach the catalog (handbook 20): the console's federated catalog, whose namespaces
 * are `<domain>.<dataset>`, and this domain's own, whose namespaces are its datasets - for an engine
 * that does not read nested namespaces. Both are the console's, which passes each request to the
 * domain with the engine's own token.
 */
function connectionOf(request: FastifyRequest, domain: Domain, enabled: boolean): CatalogConnection {
  const base = consoleBaseUrl(request);
  return { uri: `${base}/d/${domain.id}/catalog`, federatedUri: `${base}/catalog`, domain: domain.id, configured: true, enabled };
}

function logUnavailable(cause: unknown, scope: string): null {
  logger.debug({ err: cause, scope }, 'settings unavailable for the table view; using defaults');
  return null;
}

function numberOr(value: unknown, fallback: number): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

export async function registerTableRoutes(app: FastifyInstance): Promise<void> {
  /**
   * The Table tab, for one of the materialization's tables: the control plane's record of it
   * (commits, maintenance, the committer's live view), and what the console reads from the table
   * itself — snapshots, files by partition, properties. Either half may be missing; the other still
   * answers.
   */
  app.get('/materializations/:materializationId/tables/:table', { preHandler: permit('materializations.view_details') }, async (request): Promise<TableView> => {
    const id = materializationId(request);
    const detail = await domainOf(request).live.getDetail(id);
    const spec = tableSpec(detail, (request.params as { table?: string }).table ?? '');
    const table = spec.name;
    // Snapshots, files and maintenance are anyone's who may see the materialization; what describes
    // the table's fields is only for those who may see its dataset's schemas.
    const schemaVisible = can(authOf(request).state.principal, 'dataset.view_schema', spec.dataset_id);
    const domain = domainOf(request);
    const [record, settings] = await Promise.all([tableRecord(request, id, table), tableDefaults(domain)]);
    const location = record?.location ?? detail.tables.find((summary) => summary.name === table)?.location ?? null;
    let metadata: TableMetadataView | null = null;
    let metadataError: string | null = null;
    if (!location) metadataError = 'the table has not been created yet';
    else if (dataAccessMode() === 'disabled') metadataError = 'data access is turned off for this console (Settings › Data explorer)';
    else {
      try {
        const table = await loadTable(location);
        if (!table) metadataError = 'the table has not been created yet';
        else metadata = describeTable(table.metadataLocation, table.metadata, await snapshotCatalog(table.metadata), settings, schemaVisible);
      } catch (cause) {
        logger.warn({ err: cause, domain: domain.id, materializationId: id, table }, 'table metadata unreadable');
        metadataError = unreadableTable(location, cause).message;
      }
    }
    return { materializationId: id, table, record, metadata, metadataError, connection: connectionOf(request, domain, settings.catalogEnabled) };
  });

  /**
   * Run a maintenance job now — compaction, snapshot expiry, orphan cleanup or a reprotect — on one
   * table, or on every table, kept ones included. A reprotect also needs Choose data protection on
   * each table's dataset, which the control plane checks as the person asking. The maintenance app
   * runs it, whether or not the runtime runs, and reports back.
   */
  app.post('/materializations/:materializationId/maintenance', { preHandler: permit('materializations.maintain') }, async (request, reply) => {
    const id = materializationId(request);
    const body = parseBody(maintenanceSchema, request.body);
    const table = body.table ? tableSpec(await domainOf(request).live.getDetail(id), body.table).name : null;
    const accepted = await domainOf(request).controlPlane.maintain(callerOf(request), id, body.job, table, { purge: body.purge });
    domainOf(request).live.wake();
    return reply.code(202).send(accepted);
  });

  /** Where external engines reach the catalog, for the Connect panel and the access-token page. */
  app.get('/catalog/connection', { preHandler: permitAnywhere('catalog.read') }, async (request): Promise<CatalogConnection> => {
    const domain = domainOf(request);
    return connectionOf(request, domain, (await tableDefaults(domain)).catalogEnabled);
  });
}

async function tableRecord(request: FastifyRequest, id: string, table: string): Promise<CpTable | null> {
  try {
    return await domainOf(request).controlPlane.table(callerOf(request), id, table);
  } catch (cause) {
    if (cause instanceof ConsoleError && cause.statusCode === 404) return null;
    throw cause;
  }
}

/**
 * Table properties that describe its fields, which only people who may see the dataset's schemas see:
 * the fork map, what the table protects (its fields' paths and kinds, handbook 22), and any of
 * Iceberg's `schema.*`.
 */
export function describesSchema(key: string): boolean {
  return key === FORK_MAP_PROPERTY || key === PROTECTION_PROPERTY || key.startsWith('schema.');
}

function describeTable(
  metadataLocation: string,
  metadata: TableMetadata,
  snapshot: SnapshotCatalog,
  settings: Defaults,
  schemaVisible: boolean,
): TableMetadataView {
  const schema = currentSchema(metadata);
  const nameOf = (fieldId: number): string =>
    schema.fields.find((field) => field.id === fieldId)?.name ?? schemaById(metadata, 0)?.fields.find((field) => field.id === fieldId)?.name ?? `field ${fieldId}`;
  const spec = metadata.partitionSpecs.find((candidate) => candidate.specId === metadata.defaultSpecId);
  const order = metadata.sortOrders.find((candidate) => candidate.orderId === metadata.defaultSortOrderId);

  const partitions = new Map<string, TablePartitionRow>();
  const filesOf = new Map<string, DataObject[]>();
  const totals = { files: 0, bytes: 0, records: 0, smallFiles: 0, deleteFiles: snapshot.deleteFiles };
  for (const object of snapshot.objects) {
    const key = object.partitionKey ?? '';
    const row = partitions.get(key) ?? { key: object.partitionKey, files: 0, bytes: 0, records: 0, smallFiles: 0, compactionDue: false };
    const small = object.bytes < settings.smallFileBytes;
    row.files += 1;
    row.bytes += object.bytes;
    row.records += object.rows ?? 0;
    if (small) row.smallFiles += 1;
    partitions.set(key, row);
    const files = filesOf.get(key);
    if (files) files.push(object);
    else filesOf.set(key, [object]);
    totals.files += 1;
    totals.bytes += object.bytes;
    totals.records += object.rows ?? 0;
    if (small) totals.smallFiles += 1;
  }
  // Due as the maintenance app's planner decides it: what is still being written waits.
  const granularity = partitionGranularity(metadata);
  const now = Date.now();
  for (const [key, row] of partitions) row.compactionDue = compactionDue(filesOf.get(key) ?? [], row.key, granularity, settings, now);

  return {
    location: metadata.location,
    metadataLocation,
    tableUuid: metadata.tableUuid,
    formatVersion: metadata.formatVersion,
    lastUpdatedAt: metadata.lastUpdatedMs ? new Date(metadata.lastUpdatedMs).toISOString() : null,
    currentSnapshotId: metadata.currentSnapshotId,
    currentSchemaId: metadata.currentSchemaId,
    partitionSpec: spec && spec.fields.length > 0 ? spec.fields.map((field) => `${field.transform}(${nameOf(field.sourceId)})`).join(', ') : 'unpartitioned',
    sortOrder:
      order && order.fields.length > 0
        ? order.fields.map((field) => `${field.transform === 'identity' ? nameOf(field.sourceId) : `${field.transform}(${nameOf(field.sourceId)})`} ${field.direction.toUpperCase()} ${field.nullOrder.replace('-', ' ').toUpperCase()}`).join(', ')
        : 'unsorted',
    properties: Object.fromEntries(
      Object.entries(metadata.properties)
        .filter(([key]) => schemaVisible || !describesSchema(key))
        .sort(([a], [b]) => a.localeCompare(b)),
    ),
    snapshots: [...metadata.snapshots].reverse().slice(0, SNAPSHOT_ROWS).map((item) => snapshotRow(item, metadata.currentSnapshotId)),
    snapshotCount: metadata.snapshots.length,
    partitions: [...partitions.values()].sort((a, b) => (b.key ?? '').localeCompare(a.key ?? '')),
    totals,
    smallFileBytes: settings.smallFileBytes,
  };
}

function snapshotRow(snapshot: TableMetadata['snapshots'][number], currentSnapshotId: string | null): TableSnapshotRow {
  const summary = snapshot.summary;
  const count = (key: string): number | null => (summary[key] === undefined ? null : Number(summary[key]));
  let offsets: Record<string, number> | null = null;
  if (summary['materializer.offsets']) {
    try {
      offsets = JSON.parse(summary['materializer.offsets']) as Record<string, number>;
    } catch {
      offsets = null;
    }
  }
  return {
    snapshotId: snapshot.snapshotId,
    parentSnapshotId: snapshot.parentSnapshotId,
    sequenceNumber: snapshot.sequenceNumber,
    committedAt: new Date(snapshot.timestampMs).toISOString(),
    operation: summary.operation ?? 'unknown',
    materializerOperation: summary['materializer.operation'] ?? null,
    schemaId: snapshot.schemaId,
    addedFiles: count('added-data-files'),
    addedRecords: count('added-records'),
    deletedFiles: count('deleted-data-files'),
    deletedRecords: count('deleted-records'),
    totalFiles: count('total-data-files'),
    totalRecords: count('total-records'),
    totalBytes: count('total-files-size'),
    offsets,
    validThrough: summary['materializer.valid-through'] ?? null,
    runtime: summary['materializer.runtime'] ?? null,
    current: snapshot.snapshotId === currentSnapshotId,
  };
}

/** Test seam. */
export function resetTableDefaults(): void {
  defaults.clear();
}
