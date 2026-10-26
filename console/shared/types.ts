/**
 * Wire contracts shared by the console server and browser client.
 *
 * Types prefixed `Cp` mirror payloads returned by the control-plane WebView API and must stay
 * additive-only, exactly like the API they describe.
 */

export const LIFECYCLE_PHASES = [
  'requested',
  'discovering',
  'discovered',
  'deploying',
  'running',
  /** Recorded by control planes before `stopped` existed; nothing requests it any more. */
  'paused',
  /** The runtime is scaled to zero replicas; resuming scales it back to its replica count. */
  'stopped',
  'deleting',
  'deleted',
  'failed',
  'degraded',
] as const;

export type LifecyclePhase = (typeof LIFECYCLE_PHASES)[number];

/**
 * Terminal phase for a materialization whose runtime and Kafka resources are gone.
 * Records linger until the control-plane retention window elapses.
 */
export function isDeletedPhase(item: {
  observed_phase: LifecyclePhase;
  desired_phase: LifecyclePhase;
}): boolean {
  return item.observed_phase === 'deleted' && item.desired_phase === 'deleted';
}

export type RuntimeHealthLevel = 'healthy' | 'degraded' | 'unknown' | 'unreachable';

export interface CpListItem {
  materialization_id: string;
  /** Its label: free text, not unique, and its id when it was given none. */
  name: string;
  /** The topics it reads, in their order: one table each. */
  source_topics: string[];
  /** Its tables, as SQL reads them (`<dataset>.<table>`), in the same order. */
  tables: string[];
  /** The datasets its tables are in, kept ones included, by name. */
  datasets: string[];
  destination_uri: string;
  desired_phase: LifecyclePhase;
  observed_phase: LifecyclePhase;
  phase_reason: string;
  updated_at: string;
  storage_size?: string;
  replicas?: number;
  consumer_lag?: number | null;
}

export interface CpListResponse {
  items: CpListItem[];
  limit: string;
  status?: string;
  returned: number;
  /** The search, when the list was searched. */
  q?: string;
}

/**
 * Why a topic is hidden from the topic picker by default: `kafka` is broker-internal,
 * `materializer` belongs to the control plane or one of its runtimes, and `stream` is state another
 * stream processor keeps in Kafka. Absent for an ordinary data topic.
 */
export type TopicCategory = 'kafka' | 'materializer' | 'stream';

export interface CpTopicListItem {
  topic: string;
  partitions: number;
  replication_factor: number;
  category?: TopicCategory;
}

export interface CpTopicList {
  items: CpTopicListItem[];
  returned: number;
  fetched_at: string;
  stale?: boolean;
  error?: string;
}

export interface TopicSummary {
  name: string;
  partitions: number;
  replicationFactor: number;
  category?: TopicCategory;
}

/** `GET /api/topics`: every topic in the cluster, sorted by name. */
export interface TopicCatalogResponse {
  topics: TopicSummary[];
  /** When Kafka was last asked, as reported by the control plane. */
  fetchedAt: string;
  /** The list could not be refreshed and is an earlier copy; `error` says why. */
  stale: boolean;
  error: string | null;
}

export interface CpRuntimeHealth {
  level: RuntimeHealthLevel;
  reason: string | null;
  message: string | null;
}

export interface CpMemberProgress {
  rows_written: number;
  bytes_written: number;
  files_written: number;
  records_skipped: number;
  reported_at?: string;
  health?: RuntimeHealthLevel;
  health_reason?: string;
  phase_reason?: string;
  phase_message?: string;
  last_error?: string;
  /** Values this member's seals wrote to `_unmapped` since it started. */
  unmapped_values?: number;
  /** Data files this member uploaded that no commit covers yet. */
  files_pending_commit?: number;
  /** Live runtime-settings revision this member runs; absent from runtimes that predate them. */
  settings_revision?: number;
}

/** Records skipped for one reason, cumulative since creation, with the latest occurrence. */
export interface CpSkipReason {
  category: string;
  count: number;
  last_reason: string;
  last_seen_at?: string;
  last_partition?: number;
  last_offset?: number;
  last_schema_version?: number;
  last_member_id?: string;
}

export interface CpRuntimeProgress {
  rows_written?: number;
  bytes_written?: number;
  files_written?: number;
  records_skipped?: number;
  /** The highest offset the members handled, per table and source partition. */
  latest_offsets?: Record<string, Record<string, number>>;
  /** Monotonic totals that survive runtime pod restarts. Prefer these for reporting. */
  cumulative_rows_written?: number;
  cumulative_bytes_written?: number;
  cumulative_files_written?: number;
  cumulative_records_skipped?: number;
  cumulative_unmapped_values?: number;
  /** When a member last reported values going to `_unmapped`. */
  unmapped_last_seen_at?: string;
  /** Values that went to each table's `_unmapped`, and when one last did. */
  cumulative_unmapped_by_table?: Record<string, number>;
  unmapped_last_seen_by_table?: Record<string, string>;
  /** Each table as the committer member last reported it, by table name. */
  tables?: Record<string, CpLiveTable>;
  /** Last heartbeat from each runtime member, keyed by member id. */
  member_progress?: Record<string, CpMemberProgress>;
  /** Most frequent first. Only covers skips reported by runtimes that track reasons. */
  skip_reasons?: CpSkipReason[];
}

/** What the committer member reports about one table on its heartbeat (spec §7.12). */
export interface CpLiveTable {
  /** The member running the committer. */
  member_id?: string;
  table?: string;
  source_topic?: string;
  metadata_location?: string;
  current_snapshot_id?: string | null;
  snapshot_at?: string | null;
  sequence_number?: number | null;
  total_records?: number;
  total_files?: number;
  total_bytes?: number;
  schema_id?: number;
  committed_offsets?: Record<string, number>;
  valid_through?: string | null;
  snapshots?: number;
  last_commit_at?: string | null;
  last_commit_duration_ms?: number | null;
  pending_files?: number;
  /** Reported by runtimes before 2.0, which ran their own maintenance; see `CpTableSummary.maintenance_job`. */
  small_files?: number | null;
  last_compaction_at?: string | null;
  maintenance?: { job: string; started_at: string; partition: string | null; attempt: number } | null;
}

/** The maintenance job running on a table, as the maintenance app reported starting it. */
export interface CpRunningMaintenance {
  job: string;
  started_at: string;
  partition: string | null;
  attempt: number;
  /** The maintenance app's member running it. */
  member_id: string | null;
}

/** How far behind its topic's end the runtime is on one source topic. */
export interface CpTopicLag {
  total_lag?: number;
  partition_lag?: Record<string, number>;
  end_offsets?: Record<string, number>;
  partitions?: number;
  error?: string;
}

/** The runtime's lag on each source topic, and in all. */
export interface CpConsumerLag {
  total_lag?: number;
  topics?: Record<string, CpTopicLag>;
  measured_at?: string;
  /** Each topic that could not be measured, and why. */
  error?: string;
}

export interface CpResourceCondition {
  status: 'True' | 'False' | 'Unknown';
  reason?: string | null;
  message?: string | null;
  last_transition_time?: string | null;
}

/** One runtime member's pod: how often its container restarted, and why. */
export interface CpRuntimeMemberState {
  name: string;
  restarts: number;
  /** `OOMKilled`, `Error`, ... */
  last_exit_reason?: string;
  last_exit_code?: number;
  last_exited_at?: string;
  /** `CrashLoopBackOff` while Kubernetes holds off restarting it. */
  waiting_reason?: string;
}

export interface CpResourceObservation {
  namespace: string;
  kaspr_app_name: string;
  resource_exists: boolean;
  managed_agent_states: Record<string, string>;
  metadata_generation?: number;
  observed_generation?: number;
  ready_condition?: CpResourceCondition | null;
  progressing_condition?: CpResourceCondition | null;
  rollout_in_progress?: boolean;
  available_members?: string;
  rebalance_required?: boolean;
  rebalancing_members?: string;
  managed_agents_total?: number;
  managed_agents_running?: number;
  managed_agents_paused?: number;
  managed_agents_unknown?: number;
  /** Each member's pod as the control plane last read it; absent when it could not read them. */
  members?: CpRuntimeMemberState[];
}

/**
 * One of a materialization's tables: the topic it holds, the dataset it is in, and its name. SQL reads
 * it as `<dataset>.<name>`; neither part changes once the table exists.
 */
export interface CpTableSpec {
  name: string;
  source_topic: string;
  /** The dataset's name: the table's Iceberg namespace. */
  dataset: string;
  dataset_id: string;
}

/** A table as a request names it: a topic, the dataset its table goes in, and, optionally, its name. */
export interface TableRequest {
  source_topic: string;
  /** Required for a topic that has no table yet; a topic that has one keeps it. */
  dataset?: string;
  /** Omitted, the name derived from the topic (`tableNameForTopic`). */
  name?: string;
}

/** A change of topics that adds tables, waiting for the directory to claim their names. */
export interface CpPendingTopicChange {
  request_id: string;
  claim_request_id: string;
  submitted_at: string;
  added: CpTableSpec[];
}

export interface CpMaterialization {
  materialization_id: string;
  /** Its label: free text, not unique, of no meaning in SQL; its id when it was given none. */
  name: string;
  /** The tables its runtime writes: one per source topic, in the order of the topics. Edit adds and removes topics. */
  tables: CpTableSpec[];
  /**
   * Tables kept read-only after their topics were removed: still in the namespace, no longer written.
   * Adding a kept table's topic again writes it again. Absent from control planes before 2.0.
   */
  retired_tables?: CpTableSpec[];
  source_topics: string[];
  destination_uri: string;
  runtime_mode: string;
  namespace: string;
  resolved_runtime_name?: string | null;
  desired_phase?: LifecyclePhase;
  resource_profile?: string | null;
  storage_size?: string;
  replicas?: number;
  /** Absent from control planes that predate the setting, where tombstones count as errors. */
  skip_tombstones?: boolean;
  /**
   * Whether the control plane may run its runtime with more members than `replicas` while it is far
   * behind its topics. Absent before 4.4, which never scale a runtime.
   */
  autoscaling?: boolean;
  /**
   * Always true now that every materialization has a live tail; a control plane before 4.3 says
   * false for one switched off. Absent before 4.1, which have no tail.
   */
  live_tail?: boolean;
  /** How the table is partitioned: by the day or hour of the Kafka timestamp, or not at all. */
  partition_granularity?: PartitionGranularity;
  created_at?: string;
  updated_at?: string;
  created_by?: string | null;
  /** The account that created it; absent for materializations created before accounts existed. */
  created_by_id?: string | null;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  spec_version?: number;
  /** The datasets its tables are in, kept ones included, by name. */
  datasets: string[];
  /** A change of topics waiting for its new tables' names; at most one at a time. */
  pending_topic_change?: CpPendingTopicChange | null;
}

export interface CpStatus {
  desired_phase: LifecyclePhase;
  observed_phase: LifecyclePhase;
  phase_reason: string;
  phase_message: string | null;
  last_transition_time: string;
  last_heartbeat: string | null;
  last_resource_observation_time: string | null;
  observation_source?: string;
  /** Version of the materializer package the runtime last reported running. */
  runtime_version?: string | null;
  last_error?: string | null;
  runtime_health: CpRuntimeHealth;
  runtime_progress?: CpRuntimeProgress;
  consumer_lag?: CpConsumerLag;
  resource_observation?: CpResourceObservation;
}

export interface CpRuntimeBinding {
  kaspr_app_name: string;
  namespace: string;
  managed_agent_names: string[];
  runtime_mode?: string;
  template_version?: string;
  /** The topics the applied runtime reads. */
  source_topics?: string[];
  last_applied_spec_hash?: string;
  last_apply_operation_id?: string;
  created_at?: string | null;
  updated_at?: string | null;
}

/**
 * How the control plane scaled a materialization's runtime: more members than its own `replicas`
 * while it is far behind its topics, its own count again once caught up.
 */
export interface CpAutoscale {
  /** The materialization's own switch (`CpMaterialization.autoscaling`). */
  enabled: boolean;
  own_replicas: number;
  /** The members its runtime runs while scaled up; null at its own count. */
  replicas: number | null;
  /** Why it was scaled up, in words for a person; null at its own count. */
  reason: string | null;
  /** When the count last changed, up or back; null if it never has. */
  changed_at: string | null;
  /** Seconds to catch up at the pace last measured; null while there is no telling. */
  eta_seconds: number | null;
  /** The last lag the decision read, and when it was measured. */
  lag: number | null;
  measured_at: string | null;
}

export interface CpDetail {
  materialization: CpMaterialization;
  status: CpStatus;
  runtime_binding: CpRuntimeBinding | null;
  /** Absent before 4.4. */
  autoscale?: CpAutoscale;
  /**
   * The tables created so far: the written ones in the order of the topics, the kept ones next, then
   * any being dropped or dropped.
   */
  tables: CpTableSummary[];
}

/** Every table a materialization has in its datasets: the ones its runtime writes, then the kept ones. */
export function readableTables(materialization: Pick<CpMaterialization, 'tables' | 'retired_tables'>): CpTableSpec[] {
  return [...materialization.tables, ...(materialization.retired_tables ?? [])];
}

/**
 * A table's place in its materialization: written by the runtime; kept read-only after its topic was
 * removed; being dropped; or dropped with object deletion off, which leaves its files where they were.
 */
export const TABLE_STATES = ['active', 'retired', 'dropping', 'dropped'] as const;
export type TableState = (typeof TABLE_STATES)[number];

/** What becomes of the table of a topic an edit removes: dropped with its data, or kept read-only. */
export type RemovedTableAction = 'drop' | 'keep';

/** `PATCH /materializations/{id}`: the settings that can change after creation, each optional. */
export interface MaterializationUpdate {
  /** A new label; empty, the materialization goes by its id. */
  name?: string;
  replicas?: number;
  /** Turned off, a runtime scaled up goes back to `replicas` at once. */
  autoscaling?: boolean;
  /** The topics to read from now on, in order: a new one with the dataset (and name) of its table. */
  tables?: TableRequest[];
  /** For each table whose topic is removed, `drop` or `keep`; may also `drop` a kept table. */
  removed_tables?: Record<string, RemovedTableAction>;
}

/** What an accepted change of topics does to the tables, as the control plane planned it. */
export interface CpTopicChange {
  tables: CpTableSpec[];
  added: CpTableSpec[];
  /** Kept tables written again. */
  resumed: CpTableSpec[];
  kept: CpTableSpec[];
  dropped: CpTableSpec[];
}

export const PARTITION_GRANULARITIES = ['hour', 'day', 'month', 'none'] as const;
export type PartitionGranularity = (typeof PARTITION_GRANULARITIES)[number];

export interface CpTableError {
  reason: string;
  message: string;
  occurred_at: string;
}

/** The detail view's summary of one of a materialization's tables. */
export interface CpTableSummary {
  /** The dataset it is in: its Iceberg namespace. */
  dataset: string;
  dataset_id: string;
  name: string;
  source_topic: string;
  /** `<dataset>.<name>`, as SQL names it. */
  identifier: string;
  location: string;
  partition_granularity: PartitionGranularity;
  schema_id: number | null;
  current_snapshot_id: string | null;
  last_commit_at: string | null;
  valid_through: string | null;
  totals: { records?: number; files?: number; bytes?: number };
  last_error: CpTableError | null;
  /** Absent from control planes before 2.0, whose tables are all written. */
  state?: TableState;
  /** The compaction or orphan cleanup the maintenance app is running on it, if any. */
  maintenance_job?: CpRunningMaintenance | null;
  /** Whether the person asking may see its schema, on its dataset; absent when nobody asked. */
  readable?: boolean;
  /** Whether the person asking may query its rows, on its dataset. */
  queryable?: boolean;
}

export const MAINTENANCE_JOBS = ['compaction', 'expire_snapshots', 'orphan_cleanup'] as const;
export type MaintenanceJob = (typeof MAINTENANCE_JOBS)[number];

export interface CpMaintenanceRun {
  job: MaintenanceJob | string;
  completed_at: string;
  outcome: 'succeeded' | 'failed' | 'skipped' | 'abandoned' | string;
  duration_ms: number | null;
  snapshot_id: string | null;
  counts: Record<string, number>;
  message: string | null;
  partition: string | null;
  /** When a long job started; matches `CpRunningMaintenance.started_at`. */
  started_at?: string | null;
}

/** `GET /materializations/{id}/tables/{table}`: the control plane's record of a table (spec §9.3). */
export interface CpTable extends CpTableSummary {
  materialization_id: string;
  table_uuid: string;
  metadata_location: string | null;
  sequence_number: number | null;
  last_commit_duration_ms: number | null;
  offsets: Record<string, number>;
  snapshots: number | null;
  maintenance: CpMaintenanceRun[];
  created_at: string;
  updated_at: string;
  live: CpLiveTable | null;
}

/** One snapshot of a table, as the Table tab lists it. */
export interface TableSnapshotRow {
  snapshotId: string;
  parentSnapshotId: string | null;
  sequenceNumber: number;
  committedAt: string;
  /** `append` for the runtime's commits, `replace` for a compaction; another engine may write others. */
  operation: string;
  schemaId: number | null;
  addedFiles: number | null;
  addedRecords: number | null;
  deletedFiles: number | null;
  deletedRecords: number | null;
  totalFiles: number | null;
  totalRecords: number | null;
  totalBytes: number | null;
  /** Committed Kafka offsets by partition, as of this snapshot. */
  offsets: Record<string, number> | null;
  validThrough: string | null;
  /** The runtime that committed it, for the materializer's own commits. */
  runtime: string | null;
  current: boolean;
}

/** The data files of one table partition (a day, or an hour), in the current snapshot. */
export interface TablePartitionRow {
  /** `2026-10-17` (day), `2026-10-17-10` (hour), `__null__` for rows without a timestamp; null when unpartitioned. */
  key: string | null;
  files: number;
  bytes: number;
  records: number;
  /** Files below the runtimes' small-file size, which compaction would merge. */
  smallFiles: number;
  /** Enough small files that the next compaction pass would take this partition. */
  compactionDue: boolean;
}

/** What the console read from the table itself: its metadata file and current snapshot. */
export interface TableMetadataView {
  location: string;
  metadataLocation: string;
  tableUuid: string;
  formatVersion: number;
  lastUpdatedAt: string | null;
  currentSnapshotId: string | null;
  currentSchemaId: number;
  partitionSpec: string;
  sortOrder: string;
  properties: Record<string, string>;
  /** Newest first, at most 200. */
  snapshots: TableSnapshotRow[];
  snapshotCount: number;
  /** Newest partition first. */
  partitions: TablePartitionRow[];
  totals: { files: number; bytes: number; records: number; smallFiles: number; deleteFiles: number };
  /** The size below which a file counts as small: the runtimes' `compaction_small_file_bytes`. */
  smallFileBytes: number;
}

/** Where external engines reach the Iceberg REST catalog, for the Connect panel. */
export interface CatalogConnection {
  /**
   * This domain's own catalog, through the console: `<console>/d/<domain>/catalog`, without `/v1`.
   * Its namespaces are the domain's datasets, for an engine that does not read nested namespaces.
   */
  uri: string;
  /** The federated catalog: `<console>/catalog`, whose namespaces are `<domain>.<dataset>` (handbook 20). */
  federatedUri: string;
  /** The domain the table is in: the first level of its federated namespace. */
  domain: string;
  /** Whether the URI was configured for external engines, or is the address the console itself uses. */
  configured: boolean;
  enabled: boolean;
}

/** `GET /api/materializations/{id}/tables/{table}`: the Table tab, for one table. */
export interface TableView {
  materializationId: string;
  table: string;
  /** The control plane's record of the table, from the runtime's commit and maintenance events. */
  record: CpTable | null;
  metadata: TableMetadataView | null;
  /** Why `metadata` is missing, when it is. */
  metadataError: string | null;
  connection: CatalogConnection | null;
}

export interface CpMaintenanceAccepted {
  materialization_id: string;
  request_id: string;
  job: MaintenanceJob;
  /** The table the job runs on; null for every table, one after another. */
  table: string | null;
  /** Every table it was asked for (2.0 on). */
  tables?: string[];
  accepted_at: string;
}

export interface CpAccepted {
  materialization_id: string;
  request_id: string;
  desired_phase: LifecyclePhase;
  observed_phase: LifecyclePhase;
  phase_reason: string;
  status_url: string;
  accepted_at: string;
  /** Its label after the request. */
  name?: string;
  /** A create: each topic's table, in its dataset and with the name it is claimed under. */
  tables?: CpTableSpec[];
  /** An update that changes the topics: what it does to each table. */
  topic_change?: CpTopicChange;
  /** A create, or an update adding tables: the directory request claiming the new tables' names. */
  claim_request_id?: string;
  claim_status_url?: string;
  /**
   * Set by the console, which waits for the claim: `applied` once the names are the materialization's,
   * `pending` when the directory had not answered in time. A refused claim is an error instead.
   */
  claim_status?: 'applied' | 'pending';
}

/** What a field of the table's Iceberg schema is (spec §9.3). */
export type SchemaFieldKind = 'value' | 'container' | 'json' | 'metadata';
/** A fork-family member's state: written to now, replaced by a newer fork, or collapsed to JSON. */
export type SchemaFieldState = 'current' | 'superseded' | 'collapsed';

export interface CpSchemaField {
  field_id: number;
  /** The physical path in the Iceberg schema; `[]` stands for a list's element. */
  path: string[];
  /** The logical name people read: `customer.email`, `items[].sku`, one name for every fork. */
  name: string;
  /** The Iceberg type as text: `long`, `timestamptz`, `struct`, `list<string>`. */
  type: string;
  required: boolean;
  kind: SchemaFieldKind;
  state: SchemaFieldState;
  fork_version: number;
  fork_of?: number | null;
  since_schema_id?: number | null;
  doc?: string | null;
}

export interface CpSchemaChange {
  kind: 'added' | 'forked' | 'collapsed' | 'rejected' | string;
  path: string[];
  field_id?: number | null;
  physical_path?: string[] | null;
  type?: string | null;
  fork_version?: number | null;
  reason?: string | null;
}

export interface CpSchema {
  materialization_id: string;
  /** The table this is the schema of, its dataset, and the topic it holds. */
  table: string;
  dataset: string;
  dataset_id: string;
  source_topic: string;
  schema_id: number | null;
  since: string | null;
  metadata_location: string | null;
  field_count: number;
  fields: CpSchemaField[];
  changes: CpSchemaChange[];
  /** Values that went to `_unmapped`; only on the current schema. */
  unmapped?: { total_values: number; last_seen_at: string | null };
}

export interface CpSchemaVersionRef {
  schema_id: number;
  since: string | null;
  field_count: number;
  change_counts: Record<string, number>;
  current: boolean;
}

export interface CpSchemaVersions {
  materialization_id: string;
  table: string;
  dataset: string;
  dataset_id: string;
  source_topic: string;
  current_schema_id: number | null;
  versions: CpSchemaVersionRef[];
  returned: number;
}

/** Console-derived types below. */

/** A record's Kafka timestamp lies between two instants, both included (ISO-8601, UTC). */
export interface TimeSpan {
  from: string;
  to: string;
}

/**
 * When a table holds values of one of its fields: the earliest and the latest record holding one,
 * by their Kafka timestamps, as tight as the table's files - a day in a day-partitioned table.
 */
export interface FieldTimes {
  first: TimeSpan | null;
  last: TimeSpan | null;
  /** Data files holding a value; null for a struct, list or map, whose values are its fields'. */
  files: number | null;
  /** Its values, a list's elements each counted; null for a struct, list or map. */
  values: number | null;
}

/**
 * `GET /api/materializations/:id/tables/:table/schema/timeline`: when each of a table's fields
 * appeared in its records and when it last did, read from the table's manifests - what dates a
 * schema change by when it happened rather than by when the runtime met it.
 */
export interface SchemaTimeline {
  materializationId: string;
  table: string;
  /** False, with the reason, while the table cannot be read or holds nothing yet. */
  available: boolean;
  reason: string | null;
  /** The snapshot the dates were read from: the table's current one. */
  snapshotId: string | null;
  /** How the table is partitioned, which bounds how tight a date can be. */
  granularity: PartitionGranularity;
  /** The span of Kafka timestamps the table holds. */
  dataFrom: string | null;
  dataThrough: string | null;
  /** By field id, as text; a field the table holds no value of is absent. */
  fields: Record<string, FieldTimes>;
}

export interface ThroughputSample {
  /** Epoch milliseconds. */
  t: number;
  rows: number;
  bytes: number;
  files: number;
  skipped: number;
  /**
   * Rates as of the sample, from each runtime member's own heartbeats (see `server/live/rates.ts`);
   * null until every member has reported twice.
   */
  rowsPerSecond: number | null;
  bytesPerSecond: number | null;
  /** Consumer-group lag reported for the source topic at sample time; null when unknown. */
  lag: number | null;
}

/** One runtime member's own throughput, measured between its own heartbeats. */
export interface MemberRate {
  rowsPerSecond: number | null;
  bytesPerSecond: number | null;
}

/** One consumer-lag measurement: records on the source topic beyond the runtime's committed offsets. */
export interface LagSample {
  /** When the control plane measured it, epoch milliseconds. */
  t: number;
  lag: number;
}

export interface MaterializationSummary extends CpListItem {
  runtimeHealth: RuntimeHealthLevel;
  lastHeartbeat: string | null;
  /** Rows in its tables' current snapshots, as their committer last reported them; null before any table is reported. */
  rowsCommitted: number | null;
  /** Bytes of those snapshots' data files. */
  tableBytes: number | null;
  tableFiles: number | null;
  /** What the runtime wrote, across restarts: more than the tables hold when a member lost writes to a restart. */
  rowsWritten: number | null;
  bytesWritten: number | null;
  filesWritten: number | null;
  recordsSkipped: number | null;
  rowsPerSecond: number | null;
  bytesPerSecond: number | null;
  agentsRunning: number | null;
  agentsTotal: number | null;
  lag: number | null;
}

export interface FleetTotals {
  materializations: number;
  running: number;
  /** Stopped materializations, and any a control plane recorded as paused. */
  stopped: number;
  failing: number;
  pending: number;
  deleted: number;
  rowsCommitted: number;
  tableBytes: number;
  tableFiles: number;
  rowsWritten: number;
  bytesWritten: number;
  filesWritten: number;
  recordsSkipped: number;
  /** Summed over the fleet; null while any materialization's runtime members have not all reported twice. */
  rowsPerSecond: number | null;
  bytesPerSecond: number | null;
}

export interface FleetSnapshot {
  generatedAt: string;
  connected: boolean;
  controlPlaneError: string | null;
  totals: FleetTotals;
  items: MaterializationSummary[];
  series: ThroughputSample[];
}

export interface DetailSnapshot {
  generatedAt: string;
  materializationId: string;
  detail: CpDetail | null;
  /** Each table's current schema, by table name, as far as it has been read. */
  schemas: Record<string, CpSchema>;
  error: string | null;
  /** Why a table's schema could not be read, by table name, when the rest of the detail is healthy. */
  schemaErrors: Record<string, string>;
  series: ThroughputSample[];
  /**
   * Rows and Parquet bytes per second: each runtime member's own rate over its recent heartbeats,
   * summed across the members still reporting. Null until every member has reported twice.
   */
  rowsPerSecond: number | null;
  bytesPerSecond: number | null;
  /** Each runtime member still reporting, by member id, with its own rates. */
  memberRates: Record<string, MemberRate>;
  /** One point per consumer-lag measurement, oldest first; denser for the last hour. */
  lagHistory: LagSample[];
}

/**
 * How a field changed between two schema versions, matched by field id.
 *
 * `added`: a new field. `forked`: a new fork of an existing path, for a type the old field could not
 * take. `collapsed`: the field a family collapsed into, or a field whose values went to JSON.
 * `type_widened`: an Iceberg type promotion. `superseded`: a newer fork took its values over.
 */
export type SchemaChangeKind =
  | 'added'
  | 'forked'
  | 'collapsed'
  | 'type_widened'
  | 'required_changed'
  | 'superseded'
  | 'removed'
  | 'unchanged';

export interface SchemaFieldChange {
  attribute: string;
  from: unknown;
  to: unknown;
}

export interface SchemaDiffEntry {
  field_id: number;
  name: string;
  path: string[];
  kind: SchemaChangeKind;
  before: CpSchemaField | null;
  after: CpSchemaField | null;
  changes: SchemaFieldChange[];
}

export interface SchemaDiff {
  materialization_id: string;
  from: number | null;
  to: number | null;
  summary: Record<SchemaChangeKind, number>;
  entries: SchemaDiffEntry[];
}

export interface QueryColumn {
  name: string;
  type: string;
  /**
   * The column carries a `json` field, whose values are JSON text. Grids and flat exports show the
   * text; nested JSON decodes it into the value it encodes.
   */
  json?: boolean;
}

export type FilterOperator =
  | 'eq'
  | 'neq'
  | 'contains'
  | 'starts_with'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'is_null'
  | 'is_not_null';

export interface BrowseFilter {
  column: string;
  operator: FilterOperator;
  value?: string;
}

/** One bucket of a column's value distribution, most frequent first. */
export interface ColumnProfileValue {
  value: string | null;
  count: number;
}

/**
 * Distribution summary for a single column, scoped to the rows of one run.
 *
 * Drives the column rail's inline profile and the filter builder's value suggestions, so it
 * always reflects what the user is looking at rather than the dataset as a whole.
 */
export interface ColumnProfile {
  column: string;
  /** True when a statement exceeded its budget, so the figures below are incomplete. */
  partial: boolean;
  total: number;
  nonNull: number;
  distinct: number;
  min: string | null;
  max: string | null;
  topValues: ColumnProfileValue[];
}

/**
 * A half-open time range over the Kafka record timestamp, in epoch milliseconds.
 *
 * `null` leaves that side unbounded, so `{ from: null, to: null }` is "All time". Relative ranges
 * such as "Last 15 minutes" are resolved to instants by the client when Run is pressed, which is
 * what pins a run to the window the user asked about rather than a moving one.
 */
export interface TimeRange {
  from: number | null;
  to: number | null;
}

/**
 * What a materialization holds, from its table's metadata alone: one snapshot's manifests. Costs no
 * data reads.
 */
export interface DataSummary {
  materializationId: string;
  /** The table summarized. */
  table: string | null;
  available: boolean;
  /** Why `available` is false: no table yet, or nothing committed to it. */
  reason: string | null;
  objects: number;
  bytes: number;
  /** Committed rows; exact when `rowsExact`. */
  rows: number;
  rowsExact: boolean;
  /**
   * Rows in data files that hold no Kafka timestamp at all.
   *
   * They can only appear under All time: a bounded range filters on the timestamp, and these rows
   * have none — Kafka gave their records no timestamp.
   */
  untimedRows: number;
  /** Earliest and latest Kafka timestamp on record, ISO-8601; null when nothing is timed. */
  timeMin: string | null;
  timeMax: string | null;
  /** When the snapshot was committed, ISO-8601. */
  newestCommit: string | null;
  source: 'iceberg';
  /** Moment the table was read, ISO-8601. Results describe the data as of this instant. */
  asOf: string;
  /** The snapshot described: the current one, or the one asked for. Null for a table with none yet. */
  snapshotId: string | null;
  snapshotAt: string | null;
  /** The table's current snapshot when it was read; newer than `snapshotId` when that is pinned. */
  currentSnapshotId: string | null;
  /**
   * Every record with a Kafka timestamp before this instant is in the snapshot, across every
   * partition that has committed — the answer to "is my record there yet?".
   */
  validThrough: string | null;
  /** Delete files an external engine committed; the console reads append-only snapshots only. */
  deleteFiles: number;
}

/**
 * What a run over one time range would read, before anything is read.
 *
 * `rows` sums every object that overlaps the range, so it is an upper bound unless every one of
 * them lies wholly inside it, which `rowsExact` reports.
 */
export interface ScopeEstimate {
  range: TimeRange;
  objects: number;
  bytes: number;
  rows: number;
  rowsExact: boolean;
}

export type RunKind = 'browse' | 'sql';

/** Row order for a browse run. Newest is by Kafka timestamp, then offset. */
export type RunOrder = 'newest' | 'oldest';

/**
 * Everything one press of Run asks for.
 *
 * `browse` is the structured view of one table — a time range and filters over every column; `sql`
 * is a hand-written statement over any of the materialization's tables, each read by its name
 * (`FROM orders`, `iceberg.orders`), which the time range also restricts. There is no third kind,
 * and no mode a client chooses between: the request says what to read and the server decides how.
 */
export interface RunRequest {
  kind: RunKind;
  /**
   * The table the run is about: the one browsed, or the one a statement's `asOf` applies to. The
   * materialization's first table when absent.
   */
  table?: string | null;
  range: TimeRange;
  /** How the client describes the range, e.g. "Last 7 days". Echoed back for display only. */
  rangeLabel?: string | null;
  filters?: BrowseFilter[];
  order?: RunOrder;
  sql?: string;
  /** A snapshot id of `table` to read it as of; the current snapshot when absent. */
  asOf?: string | null;
  /**
   * Also read each table's records newer than its snapshot from the live tail, every
   * materialization's. On unless false; never with `asOf`.
   */
  liveTail?: boolean;
}

/**
 * What a run's live tail of one table came to.
 *
 * - `complete`: every record past the snapshot that the tail holds, up to the newest it has read.
 * - `truncated`: a cap or a paused partition stopped it short: the newest records are not shown yet.
 * - `empty`: nothing has arrived past the snapshot.
 * - `backlogged`: the runtime is too far behind the topic for the tail to apply; nothing is shown.
 * - `unavailable`: the tail could not answer (`reason` says why); the run shows committed rows only.
 * - `off`: the run asked for no tail of it: live tail is off in this console or the control plane,
 *   the materialization is stopped, or no tail app is configured (`reason`); a control plane
 *   before 4.3 may also have it off for the materialization.
 */
export const RUN_TAIL_STATUSES = ['complete', 'truncated', 'empty', 'backlogged', 'unavailable', 'off'] as const;
export type RunTailStatus = (typeof RUN_TAIL_STATUSES)[number];

/**
 * Why a tail is `off` by a switch that can be turned back on: this console's, the control plane's,
 * the materialization's (on a control plane before 4.3), or the materialization stopped. Once it is
 * on again, a new run follows the tail. Any other `off` - no tail app configured - lasts as long as
 * the console's deployment.
 */
export const RUN_TAIL_SWITCHES: readonly string[] = ['console', 'control_plane', 'materialization', 'stopped'];

/** A table's state at the tail app, as the tail reports it. */
export type RunTailState = 'seeding' | 'catching_up' | 'live' | 'paused' | 'backlogged';

export interface RunTail {
  status: RunTailStatus;
  state?: RunTailState | null;
  /**
   * Why the tail is `unavailable` or `off`: `not_ready` (warming up), `rebalancing`,
   * `unavailable_for_snapshot` (the snapshot is older than the tail keeps), `not_tailed` (the tail
   * app does not follow the table yet), `topic_unavailable`, `unreachable`, `timeout`, `forbidden`,
   * `unreadable`; `materialization`, `console`, `control_plane`, `stopped` or `not_configured` when off.
   */
  reason?: string | null;
  /** What the tail said, for a tooltip. */
  message?: string | null;
  /** Rows the run holds from the tail: the sum over its fetches. */
  rows: number;
  /** Fetches the run holds: one per refresh that found something. */
  segments: number;
  /** Per Kafka partition, the last offset the run holds: the snapshot's, or the tail's past it. */
  through: Record<string, number>;
  /** Per Kafka partition, the topic's last offset as the tail last saw it. */
  endOffsets: Record<string, number>;
  /** Records on the topic the run does not show yet: past `through`, or past the snapshot when backlogged. */
  behind: number;
  /** Rows waiting for a schema change: their values are in `_unmapped` until it is committed. */
  settling: number;
  /** How far behind the topic the tail's reading is, ms; 0 when it is at the head. */
  lagMs: number | null;
  /** The tail app's member that answered. */
  member: string | null;
  generatedAt: string | null;
}

/** One table a run reads, at the snapshot it is pinned to. */
export interface RunTableRead {
  name: string;
  snapshotId: string | null;
  snapshotAt: string | null;
  /** Read as of an older snapshot than the table's current one. */
  pinned: boolean;
  validThrough: string | null;
  /** The records past the snapshot the run read from the live tail; absent when it asked for none. */
  tail?: RunTail;
}

export type RunState = 'running' | 'succeeded' | 'failed' | 'cancelled';

/**
 * What a running run is doing, in words a status line can use.
 *
 * - `planning`: choosing data files from the snapshot's manifests.
 * - `preparing`: reading object footers before a statement binds; `objectsDone` counts them.
 * - `scanning`: reading objects to count matching rows (browse) or executing a statement (sql).
 * - `counting`: a statement's result outgrew what is kept, and the full total is being counted.
 * - `done`: terminal.
 */
export type RunPhase = 'planning' | 'preparing' | 'scanning' | 'counting' | 'done';

export interface RunProgress {
  /** Rows the scan has to read, from the manifest. */
  rowsTotal: number;
  /** Rows read so far. Exact for browse runs; DuckDB's estimate for statements. */
  rowsDone: number;
  objectsTotal: number;
  /** Objects accounted for, however: counted from the manifest or read. */
  objectsDone: number;
  /** Objects actually opened and read. Zero when the manifest answered everything. */
  objectsRead: number;
  bytesTotal: number;
  /** 0–1, or null while the engine cannot yet estimate. */
  fraction: number | null;
}

export interface RunError {
  code: string;
  message: string;
  /** Where in the statement the engine pointed, 1-based. Only for statement failures. */
  position?: StatementPosition | null;
}

/**
 * One run, as every client sees it.
 *
 * A run is the console's only unit of work: the first paint, a time range, a filter and a
 * statement are all runs, and paging and exporting read from one. It is pinned to one snapshot of
 * the table, so paging stays stable while new data keeps arriving.
 */
export interface RunSnapshot {
  id: string;
  /** The domain the run was started in: its materialization's (handbook 20). */
  domain: string;
  materializationId: string;
  request: RunRequest;
  state: RunState;
  phase: RunPhase;
  startedAt: string;
  finishedAt: string | null;
  elapsedMs: number;
  /** The run's table: the one browsed, or the statement's (`RunRequest.table`) — or the first it reads. */
  table: string;
  /** Every table the run reads, in the materialization's order. A browse run reads one. */
  tables: RunTableRead[];
  /** When the table was read. */
  asOf: string;
  /** The snapshot the run reads of `table`, and when it was committed. Null for a table with none yet. */
  snapshotId: string | null;
  snapshotAt: string | null;
  /** The run was asked for an older snapshot than the table's current one. */
  pinned: boolean;
  /** Every record before this instant is in the snapshot (`DataSummary.validThrough`). */
  validThrough: string | null;
  /** Objects the time range selected, before any rows are read. */
  scope: { objects: number; bytes: number; rows: number };
  progress: RunProgress;
  /**
   * Rows the run has matched so far — the exact total once `totalExact` is true.
   *
   * Always present: while a browse run is still counting, this is a true lower bound and grows as
   * objects are read.
   */
  rows: number;
  totalExact: boolean;
  /** Leading rows that can already be paged through, in run order. Equals `rows` once complete. */
  rowsReady: number;
  /** Result columns, once known. Browse runs know them at once; statements when they finish. */
  columns: QueryColumn[] | null;
  /**
   * Statements only: the result was larger than the console keeps, so only the first
   * `rowsRetained` rows can be paged and exported. `rows` is still the exact total.
   */
  truncated: boolean;
  rowsRetained: number | null;
  error: RunError | null;
  /** When the run's state and any stored result are discarded. */
  expiresAt: string | null;
  /** Bumped each time a refresh appends the live tail's newest records: pages read before it may differ. */
  revision: number;
  /** The live tail of the run's own table (`tables[].tail` of it); absent when it asked for none. */
  tail?: RunTail;
}

/** `POST /api/runs/:runId/refresh`: the same run with the tail's newest records, or a new run. */
export interface RunRefresh {
  /** A new run took the old one's place: the table has a newer snapshot, or the run is a statement. */
  replaced: boolean;
  run: RunSnapshot;
}

/**
 * `GET /api/materializations/:id/tables/:table/head`: the newest records on a table's topic, in the
 * table's columns, while its runtime is too far behind for the tail. Never part of a run.
 */
export interface HeadPeek {
  rows: Record<string, unknown>[];
  columns: QueryColumn[];
  peek: {
    rows: number;
    /** Per Kafka partition, the first and last offset shown. */
    offsets: Record<string, [number, number]>;
    endOffsets: Record<string, number>;
    /** Records the runtime has yet to commit. */
    behind: number;
    state: RunTailState | null;
    settling: number;
    generatedAt: string | null;
  };
}

/**
 * One page of a run's rows.
 *
 * Answered at once with whatever rows are ready. While a browse run is still counting, a page past
 * the counted rows comes back short with `complete: false`, and asking again as `rowsReady` grows
 * fills it in — which is how the first page appears while the rest of a scan is still running.
 */
export interface RunPage {
  runId: string;
  offset: number;
  rows: Record<string, unknown>[];
  columns: QueryColumn[];
  /** False while more rows may still arrive in this page's range. */
  complete: boolean;
  /** The run's row count when the page was read, and whether it was final. */
  total: number;
  totalExact: boolean;
}

/** Formats a finished run can be exported as. */
export const RUN_EXPORT_FORMATS = ['csv', 'json', 'jsonl', 'parquet'] as const;
export type RunExportFormat = (typeof RUN_EXPORT_FORMATS)[number];

/**
 * How JSON and JSON Lines exports lay a row out.
 *
 * `nested` rebuilds the record from the column paths — `{ "customer": { "email": … } }` — and
 * decodes `json` fields; `flat` keeps one key per column — `{ "customer.email": … }` — exactly as
 * the grid shows it. CSV and Parquet are always flat.
 */
export const RUN_EXPORT_SHAPES = ['nested', 'flat'] as const;
export type RunExportShape = (typeof RUN_EXPORT_SHAPES)[number];

/** `GET /system/settings`: what a new materialization takes when its request leaves a setting out. */
export interface CpSystemSettings {
  /** Null when the control plane has no default, and every create request must name a destination. */
  default_destination_uri: string | null;
  default_namespace: string;
  default_replicas: number;
  max_replicas: number;
  default_storage_size: string;
  /** Absent from control planes before 1.0. */
  default_partition_granularity?: PartitionGranularity;
  /** The longest materialization label. */
  label_max_length: number;
  dataset_name_max_length: number;
  table_name_max_length: number;
  /** What neither a dataset nor a table can be named. */
  reserved_names: string[];
  /** What only a dataset cannot be named: DuckDB's own catalogs and schemas. */
  reserved_namespace_names: string[];
  /** The most topics one materialization reads. */
  max_topics?: number;
  /** Whether the live tail serves anything, for every materialization (Settings › Live tail). Absent before 4.1. */
  live_tail_enabled?: boolean;
  /**
   * Whether the control plane scales runtimes far behind their topics at all (Settings ›
   * Materializations), the most members it gives one, and the catch-up time it aims for. Absent
   * before 4.4.
   */
  autoscaling_enabled?: boolean;
  autoscale_max_replicas?: number;
  autoscale_target_minutes?: number;
}

/** The control plane's defaults for a new materialization, as the create and edit dialogs show them. */
export interface MaterializationDefaults {
  destinationUri: string | null;
  namespace: string;
  replicas: number;
  maxReplicas: number;
  storageSize: string;
  partitionGranularity: PartitionGranularity;
  /** The longest materialization label. */
  labelMaxLength: number;
  datasetNameMaxLength: number;
  tableNameMaxLength: number;
  /** Lower-case words neither a dataset nor a table can be, in the control plane's own list. */
  reservedNames: string[];
  /** What only a dataset cannot be: DuckDB's own catalogs and schemas. */
  reservedNamespaceNames: string[];
  /** The most topics one materialization reads. */
  maxTopics: number;
}

/** How the control plane scales runtimes that are far behind their topics (Settings › Materializations). */
export interface ConsoleAutoscaling {
  /** Off, no runtime is scaled, whatever its materialization says. */
  enabled: boolean;
  /** The most members it gives one runtime. */
  maxReplicas: number;
  /** The catch-up time it scales for, in minutes. */
  targetMinutes: number;
}

/** Whether this console reads live tails, and how often the Data tab's Live mode looks. */
export interface ConsoleLiveTail {
  /** A tail app is configured (`CONSOLE_TAIL_URL`). */
  configured: boolean;
  /** Configured, and on in this console and in the control plane. */
  enabled: boolean;
  refreshMs: number;
  /** Most tail rows one fetch takes: a tail further short of the head than this is worth a peek. */
  maxRows: number;
}

/**
 * A domain this console serves (handbook 20): one Midstream deployment, with its control plane, its
 * apps, its lake and its directory. `id` is its URL segment, its catalog in SQL and its level of the
 * federated catalog's namespaces; `name` is what people see.
 */
export interface ConsoleDomain {
  id: string;
  name: string;
  /** The one domain whose `console` settings scope stores this console's own overrides. */
  settingsHome: boolean;
  /**
   * Whether the console can work with it: `ok`; `unreachable` while its control plane cannot be
   * reached; `misconfigured` when that control plane says it is another domain; `incompatible`
   * when it is a release this console does not work with.
   */
  status: 'ok' | 'unreachable' | 'misconfigured' | 'incompatible';
  /** Why it is not `ok`, in words. */
  statusMessage: string | null;
  /** Version of its control plane; null while it cannot be reached. */
  version: string | null;
  /**
   * The first 8 hex characters of the SHA-256 of the key it signs access tokens with; null while it
   * cannot be reached. Domains that differ refuse each other's tokens.
   */
  federationKeyId: string | null;
  /** Whether the live store reads it now; false shows its last known state as stale. */
  connected: boolean;
  liveTail: ConsoleLiveTail;
  /** Null while its control plane cannot be reached. */
  autoscaling: ConsoleAutoscaling | null;
  /** What a new materialization there starts from; null for anyone who cannot create one there. */
  materializationDefaults: MaterializationDefaults | null;
}

export interface ConsoleConfig {
  /** Every domain this console serves, in the registry's order. */
  domains: ConsoleDomain[];
  /** The id of the settings home: where the console's own settings are stored. */
  settingsHome: string;
  /**
   * Whether every reachable domain signs access tokens with the same key; false when one differs,
   * and a token made in one domain is refused in another. Null while fewer than two can say.
   */
  federationKeysAgree: boolean | null;
  dataAccessMode: 'direct' | 'cached' | 'disabled';
  /** Most rows one page request may return. */
  pageRowLimit: number;
  /** Most rows an export may contain. */
  exportRowLimit: number;
  /** Rows a statement's result keeps for paging and export; its total is always counted in full. */
  resultRowLimit: number;
  pollIntervalMs: number;
  version: string;
}

/**
 * One domain's part of the fleet, as the console's merged stream sends it: the domain's own fleet
 * snapshot, when this person may see it there.
 */
export interface DomainFleet {
  domain: string;
  name: string;
  /**
   * `ok` with a snapshot; `sign_in_required` or `forbidden` when this person may not see the
   * domain's materializations; `unavailable` while the console cannot check who they are there.
   */
  access: 'ok' | 'sign_in_required' | 'forbidden' | 'unavailable';
  snapshot: FleetSnapshot | null;
}

/** `GET /api/fleet` and the `fleet` event of `/api/stream/fleet`: every domain's fleet. */
export interface ConsoleFleet {
  generatedAt: string;
  domains: DomainFleet[];
}

/** What `GET /api/search` finds, in one domain. */
export interface SearchResult {
  domain: string;
  kind: 'materialization' | 'dataset' | 'table';
  /** A materialization's id, a dataset's name, or a table's `<dataset>.<table>`. */
  id: string;
  /** What to show: a label, a dataset's name, or `<domain>.<dataset>.<table>`. */
  label: string;
  /** A line of context: a materialization's topics, a dataset's description, a table's topic. */
  detail: string | null;
  /** The materialization a table belongs to. */
  materializationId: string | null;
}

export interface SearchResults {
  query: string;
  results: SearchResult[];
  /** Whether more matched than were returned. */
  truncated: boolean;
}

/* ---------------------------------------------------------------- Settings */

/**
 * Who a scope's settings configure. The control plane describes and validates `control-plane`
 * and `runtime`; it only stores `console`, which this console describes and validates itself.
 */
export const SETTINGS_SCOPES = ['control-plane', 'runtime', 'console'] as const;
export type SettingsScope = (typeof SETTINGS_SCOPES)[number];
/** `domain_list`: email domains, comma-separated, such as `example.com,corp.example.org`; empty for any. */
export type SettingKind = 'boolean' | 'integer' | 'gcs_uri' | 'quantity' | 'enum' | 'string' | 'domain_list';
export type SettingUnit =
  | 'ms'
  | 'seconds'
  | 'minutes'
  | 'hours'
  | 'days'
  | 'characters'
  | 'attempts'
  | 'bytes'
  | 'rows'
  | 'records'
  | 'points'
  | 'requests'
  | 'objects'
  | 'markers'
  | 'replicas'
  | 'materializations'
  | 'versions'
  | 'topics';
export type SettingValue = string | number | boolean | null;
/** Where a setting's value comes from: built in, the deployment's environment, or an override. */
export type SettingSource = 'default' | 'deployment' | 'override';

/** One catalog setting as the control plane reports it (`GET /settings/{scope}`). */
export interface CpSetting {
  key: string;
  scope: SettingsScope;
  section: string;
  label: string;
  description: string | null;
  kind: SettingKind;
  unit: SettingUnit | null;
  minimum: number | null;
  maximum: number | null;
  nullable: boolean;
  advanced: boolean;
  danger: boolean;
  effect: string | null;
  env_var: string | null;
  /** The values an `enum` setting takes; null for every other kind. */
  options?: string[] | null;
  value: SettingValue;
  source: SettingSource;
  default_value: SettingValue;
  deployment_value: SettingValue;
  deployment_set: boolean;
  overridden: boolean;
  updated_at: string | null;
  updated_by: string | null;
}

export interface CpSettingOverride {
  value: SettingValue;
  updated_at: string;
  updated_by: string;
}

/** A change the control plane accepted and then refused, e.g. because the scope changed first. */
export interface CpSettingsRejection {
  request_id: string;
  reason: string;
  message: string;
  rejected_at: string;
}

/** A setting fixed by the deployment, reported read-only. */
export interface CpDeploymentSetting {
  key: string;
  component: 'control-plane' | 'runtime';
  label: string;
  description: string | null;
  env_var: string | null;
  value: SettingValue;
}

export type RuntimeRolloutState = 'current' | 'pending' | 'stopped' | 'not_reporting' | 'unsupported';

export interface CpRuntimeRollout {
  revision: number;
  total: number;
  current: number;
  pending: number;
  stopped: number;
  not_reporting: number;
  unsupported: number;
  materializations: {
    materialization_id: string;
    name: string | null;
    state: RuntimeRolloutState;
    revision: number | null;
    members: number;
  }[];
}

export interface CpSettingsScope {
  scope: SettingsScope;
  revision: number;
  updated_at: string | null;
  updated_by: string | null;
  recent_request_ids: string[];
  last_rejection: CpSettingsRejection | null;
  /** Catalog scopes. */
  settings?: CpSetting[];
  deployment?: CpDeploymentSetting[];
  /** The runtime scope: how far the current revision has reached the runtimes. */
  rollout?: CpRuntimeRollout | null;
  /** The console scope: stored overrides only. */
  values?: Record<string, CpSettingOverride>;
}

export interface CpSettingsAccepted {
  scope: SettingsScope;
  request_id: string;
  base_revision: number;
  accepted_at: string;
  status_url: string;
}

export interface CpSettingChange {
  key: string;
  before: SettingValue;
  after: SettingValue;
  before_source: SettingSource;
  after_source: SettingSource;
}

export interface CpSettingsChangeRecord {
  scope: SettingsScope;
  revision: number;
  request_id: string;
  updated_at: string;
  updated_by: string;
  changes: CpSettingChange[];
}

export interface CpSettingsHistory {
  scope: SettingsScope;
  changes: CpSettingsChangeRecord[];
  returned: number;
  retained: number;
}

/** One setting as the Settings page shows it, whichever component it configures. */
export interface SettingView {
  scope: SettingsScope;
  key: string;
  section: string;
  label: string;
  description: string | null;
  kind: SettingKind;
  unit: SettingUnit | null;
  minimum: number | null;
  maximum: number | null;
  options: string[] | null;
  maxLength: number | null;
  nullable: boolean;
  advanced: boolean;
  danger: boolean;
  /** When a change takes effect, where that is not immediately. */
  effect: string | null;
  envVar: string | null;
  value: SettingValue;
  source: SettingSource;
  defaultValue: SettingValue;
  deploymentValue: SettingValue;
  deploymentSet: boolean;
  updatedAt: string | null;
  updatedBy: string | null;
}

export interface SettingsScopeView {
  scope: SettingsScope;
  revision: number;
  updatedAt: string | null;
  updatedBy: string | null;
  settings: SettingView[];
}

export interface DeploymentSettingView {
  component: 'control-plane' | 'runtime' | 'console';
  key: string;
  label: string;
  description: string | null;
  envVar: string | null;
  value: SettingValue;
}

export interface RuntimeRollout {
  revision: number;
  total: number;
  current: number;
  pending: number;
  stopped: number;
  notReporting: number;
  unsupported: number;
  materializations: {
    materializationId: string;
    name: string | null;
    state: RuntimeRolloutState;
    revision: number | null;
    members: number;
  }[];
}

/** Everything the Settings page shows (`GET /api/settings`). */
export interface SettingsOverview {
  generatedAt: string;
  /** Settings are stored with the control plane, so they cannot be saved while it is unreachable. */
  controlPlane: { reachable: boolean; error: string | null };
  /**
   * The scopes the page edits: the console's own on the console's Settings page, a domain's control
   * plane and runtimes on that domain's (handbook 20). The others are null.
   */
  scopes: {
    'control-plane': SettingsScopeView | null;
    runtime: SettingsScopeView | null;
    console: SettingsScopeView | null;
  };
  deployment: DeploymentSettingView[];
  rollout: RuntimeRollout | null;
  /** Changes kept per scope. */
  historyRetained: number;
}

export interface SettingsChangeInput {
  scope: SettingsScope;
  /** Settings to override, by key. */
  values: Record<string, SettingValue>;
  /** Settings to return to their deployment or default value. */
  reset: string[];
  /** The scope revision the change was made against; a newer one refuses it. */
  baseRevision: number;
}

export interface SettingsSaveRequest {
  changes: SettingsChangeInput[];
}

/** The settings after a save; `pending` names scopes whose change was accepted but not yet applied. */
export interface SettingsSaveResult extends SettingsOverview {
  pending: SettingsScope[];
}

export interface SettingsHistoryEntry {
  scope: SettingsScope;
  revision: number;
  requestId: string;
  updatedAt: string;
  updatedBy: string;
  changes: {
    key: string;
    before: SettingValue;
    after: SettingValue;
    beforeSource: SettingSource;
    afterSource: SettingSource;
  }[];
}

export interface SettingsHistory {
  changes: SettingsHistoryEntry[];
  /** Changes kept per scope. */
  retainedPerScope: number;
  /** Scopes whose history could not be read, and why. */
  errors: { scope: SettingsScope; message: string }[];
}

/** Where in a submitted statement an error was reported. Both 1-based, as an editor counts. */
export interface StatementPosition {
  line: number;
  column: number;
}

export interface ApiErrorBody {
  error: {
    code: string;
    message: string;
    details?: Record<string, unknown> & {
      /** Present when the engine could point at the failure, against the statement as submitted. */
      position?: StatementPosition;
    };
  };
}

/* ------------------------------------------------------------------------------------------------
 * Accounts, groups, roles and permissions
 *
 * `Cp*` types mirror the control plane's identity API exactly. The console's own `/api/auth/*`
 * and directory routes pass most of them through; `AuthState` is the console's view of a session.
 * --------------------------------------------------------------------------------------------- */

/** Who a request is made as: a signed-in person, a guest, or the console's service credential. */
/** `token`: an access token an external tool presents for the Iceberg catalog. */
export type PrincipalKind = 'user' | 'guest' | 'service' | 'token';

export interface CpPrincipal {
  kind: PrincipalKind;
  user_id: string | null;
  email: string | null;
  first_name: string | null;
  last_name: string | null;
  display_name: string;
  group_ids: string[];
  role_ids: string[];
  /** Every permission held, implications included. */
  permissions: string[];
  /** Holds every permission, including ones added in later releases (Administrator). */
  all_permissions: boolean;
  /**
   * By dataset id, what grants on that dataset add to `permissions`: dataset permissions only.
   * Absent from control planes before 3.0.
   */
  dataset_permissions?: Record<string, string[]>;
  /** An access token limited to some datasets: their ids. Null when it is not limited. */
  dataset_limit?: string[] | null;
  /** Signed in with a password someone else chose, which must be replaced before anything else. */
  password_change_required: boolean;
}

export interface CpSessionInfo {
  session_id: string;
  method: string | null;
  signed_in_at: string | null;
  issued_at: string | null;
  /** When the session ends unless used: the idle timeout. */
  expires_at: string | null;
  /** When the session ends however active it is. */
  session_expires_at: string | null;
  /** When a client should renew the session. */
  refresh_after: string | null;
}

export interface AuthProvider {
  id: string;
  type: string;
  label: string;
}

/** What a new password must be, so a form can say so before it is sent. */
export interface PasswordPolicy {
  min_length: number;
  max_length: number;
}

export interface CpAuthSession {
  authenticated: boolean;
  principal: CpPrincipal;
  session: CpSessionInfo | null;
  providers: AuthProvider[];
  /** Absent from control planes that predate it. */
  password_policy?: PasswordPolicy;
  /** Why a presented session could not be used, when it could not. */
  session_error?: string;
  /** Present on sign-in and renewal only. */
  token?: string;
  token_type?: string;
}

/** The console's answer to "who am I": never carries the session token. */
export interface AuthState {
  authenticated: boolean;
  principal: CpPrincipal;
  session: CpSessionInfo | null;
  providers: AuthProvider[];
  passwordPolicy: PasswordPolicy | null;
  sessionError: string | null;
}

/**
 * `GET /api/auth/session`: who the browser is in each domain - a person with a session, or a guest -
 * and how people may sign in (handbook 20). Each domain has its own session; signing in through a
 * provider starts one in every domain that accepts the person.
 */
export interface ConsoleSession {
  domains: Record<string, DomainAuth>;
  /** The single sign-on providers this console offers; password sign-in is per domain. */
  providers: SsoProvider[];
}

/** Who the browser is in one domain, or why the console cannot say. */
export interface DomainAuth {
  /** Null while the domain's control plane cannot be asked. */
  state: AuthState | null;
  /** Why the domain could not be asked, in words. */
  error: string | null;
}

export type SsoProviderId = 'entra' | 'github' | 'google' | 'oidc';

/** A provider the console signs people in with: `entra`, `github`, `google` or `oidc`. */
export interface SsoProvider {
  id: SsoProviderId;
  /** Who the provider is: "Microsoft", "GitHub", "Google", or the generic provider's own label. */
  label: string;
}

/** How one domain answered a sign-in through a provider. */
export interface SsoDomainOutcome {
  domain: string;
  name: string;
  /** `signed_in`; `refused`, with the domain's reason; `unreachable` while it cannot be asked. */
  status: 'signed_in' | 'refused' | 'unreachable';
  /** The domain's reason: `no_account`, `provider_disabled`, `email_not_allowed`, … */
  reason: string | null;
  message: string | null;
}

/** `GET /api/auth/sso/outcomes/:id`: what a sign-in through a provider did, in every domain. */
export interface SsoOutcome {
  provider: SsoProviderId;
  label: string;
  /** Why the sign-in went nowhere before any domain was asked: refused at the provider, expired. */
  error: string | null;
  domains: SsoDomainOutcome[];
}

export interface CpSessionRecord {
  session_id: string;
  method: string;
  created_at: string;
  last_seen_at: string;
  expires_at: string;
  ip: string | null;
  user_agent: string | null;
  current: boolean;
  active: boolean;
  ended_at: string | null;
  ended_reason: string | null;
}

export interface CpMySessions {
  current_session_id: string | null;
  sessions: CpSessionRecord[];
}

export type UserStatus = 'active' | 'deactivated';
export const USER_FILTERS = ['all', 'active', 'deactivated', 'locked', 'password_change'] as const;
export type UserFilter = (typeof USER_FILTERS)[number];
export const USER_SORTS = ['name', 'last_active', 'created'] as const;
export type UserSort = (typeof USER_SORTS)[number];

export interface CpGroupRef {
  group_id: string;
  name: string;
}

export interface CpRoleRef {
  role_id: string;
  name: string;
  built_in: boolean;
}

export interface CpUser {
  user_id: string;
  email: string;
  first_name: string;
  last_name: string;
  display_name: string;
  status: UserStatus;
  /** Groups the person was added to; everyone is also in All Users. */
  groups: CpGroupRef[];
  must_change_password: boolean;
  has_password: boolean;
  /** Set while failed sign-ins have locked the person's email out. */
  locked_until: string | null;
  last_sign_in_at: string | null;
  last_active_at: string | null;
  created_at: string;
  created_by: string;
  updated_at: string;
  updated_by: string;
  deactivated_at: string | null;
  deactivated_by: string | null;
  revision: number;
  /** Holds every permission. */
  administrator: boolean;
  /** Whether the person asking may change this account: they hold everything it holds. */
  manageable: boolean;
  self: boolean;
}

export interface CpUserList {
  users: CpUser[];
  counts: Record<UserFilter, number>;
  returned: number;
  can_manage: boolean;
}

export interface CpMembership {
  group_id: string;
  name: string;
  membership: 'assigned' | 'authenticated' | 'guests';
  automatic: boolean;
  roles: CpRoleRef[];
}

export interface CpPermissionSource {
  group_id: string;
  group: string;
  role_id: string;
  role: string;
}

export interface CpUserDetail extends CpUser {
  memberships: CpMembership[];
  permissions: { key: string; via: CpPermissionSource[] }[];
  all_permissions: boolean;
  failed_sign_ins: number;
  sessions: CpSessionRecord[];
  sign_in_methods: string[];
  /** Roles held on single datasets: the person's own grants, and their groups'. */
  dataset_grants: CpHeldDatasetGrant[];
}

/** Roles a person or group holds on one dataset, as their page lists them. */
export interface CpHeldDatasetGrant {
  dataset_id: string;
  dataset: string;
  principal_type: 'user' | 'group';
  principal_id: string;
  /** The group the grant is held through; null for the person's own. */
  via_group: string | null;
  roles: CpRoleRef[];
}

export interface CpGroup {
  group_id: string;
  name: string;
  description: string;
  membership: 'assigned' | 'authenticated' | 'guests';
  built_in: boolean;
  /** Roles the group must always hold. */
  locked_role_ids: string[];
  role_ids: string[];
  role_names: string[];
  /** Null for Guest, whose members are everyone not signed in. */
  member_count: number | null;
  /** Grants every permission. */
  administrators: boolean;
  revision: number;
  created_at: string;
  created_by: string;
  updated_at: string;
  updated_by: string;
  manageable: boolean;
  /** Whether the person asking holds everything the group grants, so may add people to it. */
  grantable: boolean;
}

export interface CpGroupMember {
  user_id: string;
  email: string;
  display_name: string;
  status: UserStatus;
  last_active_at: string | null;
}

export interface CpGroupDetail extends CpGroup {
  permissions: string[];
  all_permissions: boolean;
  roles: CpRoleRef[];
  /** Null when the person asking cannot see users. */
  members: CpGroupMember[] | null;
  /** Roles the group holds on single datasets. */
  dataset_grants: CpHeldDatasetGrant[];
}

export interface CpGroupList {
  groups: CpGroup[];
  can_manage: boolean;
}

export interface CpRole {
  role_id: string;
  name: string;
  description: string;
  built_in: boolean;
  /** As granted; `*` for every permission. */
  permissions: string[];
  /** With every implication added. */
  effective_permissions: string[];
  all_permissions: boolean;
  groups: CpGroupRef[];
  /** Holds only dataset permissions, so it can be granted on a dataset. */
  dataset_role?: boolean;
  revision: number;
  created_at: string;
  created_by: string;
  updated_at: string;
  updated_by: string;
}

export interface CpRoleList {
  roles: CpRole[];
  can_manage: boolean;
}

/** Where a permission holds: across the deployment, or on one dataset at a time. */
export type PermissionScope = 'deployment' | 'dataset';

export interface CpPermissionDefinition {
  key: string;
  category: string;
  label: string;
  description: string;
  implies: string[];
  danger: boolean;
  /** A dataset permission held across the deployment holds on every dataset. */
  scope: PermissionScope;
}

export interface CpPermissionCatalog {
  categories: { id: string; label: string }[];
  permissions: CpPermissionDefinition[];
}

export interface CpAuditEntry {
  entry_id: string;
  occurred_at: string;
  action: string;
  outcome: 'success' | 'failure';
  actor_id: string | null;
  actor_email: string | null;
  target_type: string | null;
  target_id: string | null;
  target_label: string | null;
  details: Record<string, unknown>;
  ip: string | null;
  user_agent: string | null;
  cursor: string;
}

export interface CpAuditPage {
  entries: CpAuditEntry[];
  returned: number;
  /** Pass as `before` for the next page; null on the last. */
  next: string | null;
  retention_days: number;
}

export interface CpIdentityAccepted {
  request_id: string;
  action: string;
  entity_type: string;
  entity_id: string;
  accepted_at: string;
  status_url: string;
}

/** What an access token may carry: reading the Iceberg catalog, and committing through it. */
export const TOKEN_PERMISSIONS = ['catalog.read', 'catalog.write'] as const;
export type TokenPermission = (typeof TOKEN_PERMISSIONS)[number];

/** A dataset, as a token names it. */
export interface CpDatasetRef {
  dataset_id: string;
  name: string;
}

/** One of the caller's access tokens. Its secret is never shown again after it is made. */
export interface CpAccessToken {
  token_id: string;
  name: string;
  permissions: string[];
  /** The datasets it is limited to; empty, every dataset its owner may read. */
  datasets: CpDatasetRef[];
  created_at: string;
  expires_at: string | null;
  revoked_at: string | null;
  revoked_by: string | null;
  active: boolean;
}

export interface CpAccessTokenList {
  tokens: CpAccessToken[];
  /** Most tokens one person may hold. */
  limit: number;
  /** Each permission a token can carry, and whether the caller has it to give, on any dataset. */
  permissions: Array<{ key: string; label: string; granted: boolean }>;
  /** The datasets a token could be limited to: those the caller reads through the catalog. */
  datasets: Array<CpDatasetRef & { permissions: string[] }>;
  /** Most datasets one token may be limited to. */
  max_datasets: number;
  default_expires_in_days: number;
  max_expires_in_days: number;
}

/** A token just made: `token` is its text, shown this once. */
export interface CpAccessTokenCreated extends CpIdentityAccepted {
  token: string;
  token_id: string;
  name: string;
  permissions: string[];
  datasets: CpDatasetRef[];
  expires_at: string;
}

export interface CpIdentityOutcome {
  request_id: string;
  status: 'pending' | 'applied' | 'rejected';
  action?: string;
  completed_at?: string;
  actor_id?: string;
  entity_type?: string | null;
  entity_id?: string | null;
  reason?: string | null;
  message?: string | null;
  field?: string | null;
  /** More about a refusal: for a claim of table names, which table and dataset it was about. */
  details?: Record<string, unknown>;
}

/** What the console answers once a change to users, groups or roles has been applied. */
export interface IdentityChangeResult {
  request_id: string;
  /** `pending` when the control plane accepted the change but had not applied it within 10 s. */
  status: 'applied' | 'pending';
  entity_type: string;
  entity_id: string;
}

/* ---------------------------------------------------------------- Datasets */

/** A person or a group, as a dataset names its owners. */
export interface CpPrincipalRef {
  type: 'user' | 'group';
  id: string;
  name: string;
  email?: string;
}

/**
 * A dataset: a named group of tables, and their Iceberg namespace. Everyone who may see datasets sees
 * every one, its owners and its tables' names; `locked` when the person asking cannot read it.
 */
export interface CpDataset {
  dataset_id: string;
  name: string;
  description: string;
  revision: number;
  created_at: string;
  created_by: string;
  updated_at: string;
  updated_by: string;
  owners: CpPrincipalRef[];
  /** Its tables' names. */
  tables: string[];
  table_count: number;
  /** The dataset permissions the person asking holds on it. */
  permissions: string[];
  locked: boolean;
}

export interface CpDatasetList {
  datasets: CpDataset[];
  returned: number;
  /** Whether the person asking may create datasets. */
  can_create: boolean;
}

/** Where a table of a dataset is: claimed but not created yet, or one of a table's states. */
export type DatasetTableState = 'pending' | TableState;

/** One of a dataset's tables, and the materialization that writes it. */
export interface CpDatasetTable {
  name: string;
  source_topic: string;
  state: DatasetTableState;
  materialization_id: string;
  /** The materialization's label; null once it is gone. */
  materialization_name: string | null;
  created_at: string | null;
  last_commit_at: string | null;
}

/** `GET /datasets/{dataset}`: the dataset, and its tables in full. */
export interface CpDatasetDetail extends Omit<CpDataset, 'tables'> {
  tables: CpDatasetTable[];
}

/** A role, as a dataset's access lists it. */
export interface CpDatasetRole {
  role_id: string;
  name: string;
  built_in: boolean;
  permissions: string[];
}

/** The roles a person or a group holds on a dataset. */
export interface CpDatasetGrant {
  principal_type: 'user' | 'group';
  principal_id: string;
  name: string;
  email: string | null;
  role_ids: string[];
  roles: CpDatasetRole[];
  updated_at: string;
  updated_by: string;
}

/** `GET /datasets/{dataset}/grants`: who may use a dataset, and the roles that can be granted on it. */
export interface CpDatasetGrantList {
  dataset: string;
  dataset_id: string;
  grants: CpDatasetGrant[];
  /** Every role that can be granted on a dataset; `grantable` when the person asking may grant it. */
  roles: Array<CpDatasetRole & { grantable: boolean }>;
  /** Whom it can be shared with: the active people, and every group but Guest. */
  candidates: {
    users: Array<{ user_id: string; name: string; email: string }>;
    groups: Array<{ group_id: string; name: string }>;
  };
}

/** What the console answers once a change to a dataset or its access has been made. */
export interface DatasetChangeResult extends IdentityChangeResult {
  /** The dataset's name. */
  name?: string;
}
