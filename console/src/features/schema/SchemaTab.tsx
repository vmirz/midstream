import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Braces, GitCompare, History, Inbox, ListTree, OctagonAlert, RefreshCw, Search, Table2, TriangleAlert } from 'lucide-react';
import { useEffect, useMemo, useState, type ReactNode } from 'react';

import { SchemaDiffView } from './SchemaDiffView';
import { SchemaFieldTable } from './SchemaFieldTable';
import { SchemaTimelineView } from './SchemaTimelineView';
import { SchemaTreeView } from './SchemaTreeView';
import { formatSpan, versionDataSpans } from './timeline';
import { useSelectedTable } from '../tables/selectedTable';
import { TablePicker } from '../tables/TablePicker';
import { PageBody } from '@/components/AppShell';
import { RelativeTime } from '@/components/Inline';
import {
  Badge,
  Button,
  EmptyState,
  Input,
  Panel,
  PanelHeader,
  SegmentedControl,
  Select,
  Skeleton,
} from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { useAuth } from '@/lib/auth';
import { formatCount, formatTimestamp } from '@/lib/format';
import { useApi } from '@/lib/domains';
import type { CpSchema, CpSchemaVersionRef, CpTableSpec, DetailSnapshot, SchemaTimeline, TimeSpan } from '@shared/types';

type Mode = 'fields' | 'changes' | 'timeline';
type FieldLayout = 'table' | 'tree';

const FIELD_LAYOUTS: { value: FieldLayout; label: ReactNode }[] = [
  {
    value: 'table',
    label: (
      <span className="flex items-center gap-1.5">
        <Table2 className="size-3.5" />
        Table
      </span>
    ),
  },
  {
    value: 'tree',
    label: (
      <span className="flex items-center gap-1.5">
        <ListTree className="size-3.5" />
        Tree
      </span>
    ),
  },
];

/**
 * The schema of one of the materialization's tables: every version it has had, its fields, and what
 * changed between two. Each table evolves on its own, as its topic's records do.
 */
export function SchemaTab({
  materializationId,
  snapshot,
}: {
  materializationId: string;
  snapshot: DetailSnapshot;
}): ReactNode {
  const { can } = useAuth();
  // Only the schemas of tables whose datasets the person may read; the others are listed, locked.
  const readable = (spec: CpTableSpec): boolean => can('dataset.view_schema', spec.dataset_id);
  const { table, tables, select } = useSelectedTable(snapshot.detail, readable);
  if (!snapshot.detail || !table) {
    return (
      <PageBody>
        <EmptyState
          title={snapshot.detail ? 'No schemas you may see' : 'Not found'}
          description={
            snapshot.detail
              ? 'Every table of this materialization is in a dataset locked to you: its schemas need a role on the dataset.'
              : 'This materialization is no longer present.'
          }
        />
      </PageBody>
    );
  }
  // Keyed by the table, so versions, comparisons and filters chosen for one never apply to another.
  return (
    <TableSchema
      key={table.name}
      materializationId={materializationId}
      snapshot={snapshot}
      table={table}
      tables={tables}
      locked={(spec) => !readable(spec)}
      onSelectTable={select}
    />
  );
}

function TableSchema({
  materializationId,
  snapshot,
  table,
  tables,
  locked,
  onSelectTable,
}: {
  materializationId: string;
  snapshot: DetailSnapshot;
  table: CpTableSpec;
  tables: CpTableSpec[];
  /** Whether a table's schema is closed to the person: its dataset is locked to them. */
  locked: (table: CpTableSpec) => boolean;
  onSelectTable: (name: string) => void;
}): ReactNode {
  const api = useApi();
  const [mode, setMode] = useState<Mode>('fields');
  const [fieldLayout, setFieldLayout] = useState<FieldLayout>('table');
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const [compareFrom, setCompareFrom] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  const [showSuperseded, setShowSuperseded] = useState(false);
  const queryClient = useQueryClient();

  const liveSchema = snapshot.schemas[table.name] ?? null;
  const currentVersion = liveSchema?.schema_id ?? null;
  const reportedVersion = snapshot.detail?.tables.find((record) => record.name === table.name)?.schema_id ?? null;
  const schemaError = snapshot.schemaErrors[table.name] ?? null;

  const versionsQuery = useQuery({
    queryKey: ['schema-versions', materializationId, table.name, currentVersion],
    queryFn: () => api.schemaVersions(materializationId, table.name),
    refetchInterval: 30_000,
  });

  const versions = versionsQuery.data?.versions ?? [];
  const activeVersion = selectedVersion ?? currentVersion;

  // When each field held values, from the table's manifests: read again once a commit moves the table.
  const currentSnapshot = snapshot.detail?.tables.find((record) => record.name === table.name)?.current_snapshot_id ?? null;
  const timelineQuery = useQuery({
    queryKey: ['schema-timeline', materializationId, table.name, currentSnapshot],
    queryFn: () => api.schemaTimeline(materializationId, table.name),
    staleTime: 60_000,
  });
  const timeline = timelineQuery.data?.available ? timelineQuery.data : null;
  const versionSpans = useMemo(
    () => (liveSchema && timeline ? versionDataSpans(liveSchema.fields, timeline.fields) : new Map<number, TimeSpan>()),
    [liveSchema, timeline],
  );

  useEffect(() => {
    if (compareFrom !== null || activeVersion === null) return;
    const previous = versions.find((entry) => entry.schema_id < activeVersion);
    if (previous) setCompareFrom(previous.schema_id);
  }, [compareFrom, activeVersion, versions]);

  const versionQuery = useQuery({
    queryKey: ['schema-version', materializationId, table.name, activeVersion],
    queryFn: () => api.schemaVersion(materializationId, table.name, activeVersion as number),
    enabled: activeVersion !== null && activeVersion !== currentVersion,
  });

  const schema = activeVersion === currentVersion ? liveSchema : (versionQuery.data ?? null);

  const fields = useMemo(() => {
    const query = search.trim().toLowerCase();
    const all = schema?.fields ?? [];
    // Payload fields in schema order, then the Kafka metadata every table has.
    const ordered = [...all.filter((field) => field.kind !== 'metadata'), ...all.filter((field) => field.kind === 'metadata')];
    return ordered
      .filter((field) => showSuperseded || field.state !== 'superseded')
      .filter(
        (field) =>
          !query ||
          field.name.toLowerCase().includes(query) ||
          field.path.join('.').toLowerCase().includes(query) ||
          field.type.toLowerCase().includes(query),
      );
  }, [schema?.fields, search, showSuperseded]);

  const supersededCount = (schema?.fields ?? []).filter((field) => field.state === 'superseded').length;

  // Which table, for a materialization of several; nothing to choose otherwise.
  const tableBar =
    tables.length > 1 ? (
      <div className="flex flex-wrap items-center gap-2 text-[12px] text-[var(--color-ink-muted)] lg:col-span-2">
        <TablePicker tables={tables} value={table.name} onChange={onSelectTable} locked={locked} />
        <span>
          The schema of topic <code className="numeric text-[var(--color-ink)]">{table.source_topic}</code>, read in SQL as{' '}
          <code className="numeric text-[var(--color-ink)]">
            {table.dataset}.{table.name}
          </code>
        </span>
      </div>
    ) : null;

  if (currentVersion === null) {
    // A reported version with no readable schema is a fetch failure, not a pending discovery.
    if (schemaError || reportedVersion !== null) {
      return (
        <PageBody className="space-y-4">
          {tableBar}
          <Panel>
            <EmptyState
              icon={<TriangleAlert className="size-6" />}
              title="Schema unavailable"
              description={
                <>
                  {reportedVersion === null
                    ? 'The console could not read the table’s schema from the control plane.'
                    : `The table is at schema ${reportedVersion}, but the console could not read it.`}
                  {schemaError ? (
                    <span className="mt-2 block text-[var(--color-phase-failed)]">{schemaError}</span>
                  ) : null}
                </>
              }
              action={
                <Button
                  onClick={() =>
                    void queryClient.invalidateQueries({ queryKey: ['schema-versions', materializationId, table.name] })
                  }
                >
                  <RefreshCw className="size-4" />
                  Retry
                </Button>
              }
            />
          </Panel>
        </PageBody>
      );
    }

    return (
      <PageBody className="space-y-4">
        {tableBar}
        <Panel>
          <EmptyState
            title="Schema not discovered yet"
            description={`The control plane samples ${table.source_topic} to infer the table’s first schema, schema 0, shortly after creation.`}
          />
        </Panel>
      </PageBody>
    );
  }

  return (
    <PageBody className="grid gap-6 lg:grid-cols-[240px_minmax(0,1fr)]">
      {tableBar}
      <Panel className="h-fit overflow-hidden">
        <PanelHeader
          title="Versions"
          description={versions.length <= 1 ? 'Every schema the table has had, kept as the runtime committed it.' : `${versions.length} schemas`}
        />
        {versionsQuery.isPending ? (
          <div className="space-y-2 p-4">
            <Skeleton className="h-10" />
            <Skeleton className="h-10" />
          </div>
        ) : (
          <ul className="max-h-[420px] overflow-y-auto">
            {versions.map((version) => {
              const active = version.schema_id === activeVersion;
              return (
                <li key={version.schema_id}>
                  <button
                    type="button"
                    onClick={() => setSelectedVersion(version.schema_id)}
                    className={cn(
                      'flex w-full flex-col gap-0.5 border-l-2 px-4 py-2.5 text-left transition-colors',
                      active
                        ? 'border-[var(--color-ink)] bg-[var(--color-raised)]'
                        : 'border-transparent hover:bg-[var(--color-raised)]',
                    )}
                  >
                    <span className="flex items-center gap-2 text-[13px] font-medium">
                      Schema {version.schema_id}
                      {version.current ? <Badge tone="accent">current</Badge> : null}
                    </span>
                    <span className="text-[11px] text-[var(--color-ink-muted)]">
                      {formatCount(version.field_count)} fields{changeSummary(version) ? ` · ${changeSummary(version)}` : ''}
                    </span>
                    <VersionDate version={version} span={versionSpans.get(version.schema_id) ?? null} timeline={timeline} />
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </Panel>

      <div className="min-w-0 space-y-4">
        {activeVersion === currentVersion && liveSchema ? <SchemaCallouts schema={liveSchema} /> : null}
        <div className="flex flex-wrap items-center justify-between gap-3">
          <SegmentedControl
            value={mode}
            onChange={setMode}
            options={[
              { value: 'fields', label: 'Fields' },
              {
                value: 'changes',
                label: (
                  <span className="flex items-center gap-1.5">
                    <GitCompare className="size-3.5" />
                    Changes
                  </span>
                ),
              },
              {
                value: 'timeline',
                label: (
                  <span className="flex items-center gap-1.5">
                    <History className="size-3.5" />
                    Timeline
                  </span>
                ),
              },
            ]}
          />

          {mode === 'fields' ? (
            <div className="flex flex-wrap items-center gap-2">
              <SegmentedControl
                value={fieldLayout}
                onChange={setFieldLayout}
                options={FIELD_LAYOUTS}
              />
              <div className="relative">
                <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-[var(--color-ink-faint)]" />
                <Input
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder="Filter fields"
                  className="w-[220px] pl-9"
                  spellCheck={false}
                />
              </div>
              {supersededCount > 0 ? (
                <Button
                  variant={showSuperseded ? 'secondary' : 'ghost'}
                  size="sm"
                  onClick={() => setShowSuperseded((current) => !current)}
                >
                  {showSuperseded ? 'Hide superseded' : `Show ${supersededCount} superseded`}
                </Button>
              ) : null}
            </div>
          ) : mode === 'timeline' ? (
            <span className="text-[12px] text-[var(--color-ink-muted)]">Every schema the table has had, by its data</span>
          ) : (
            <div className="flex items-center gap-2 text-[12px] text-[var(--color-ink-muted)]">
              <span>Compare</span>
              <Select
                value={compareFrom ?? ''}
                onChange={(event) =>
                  setCompareFrom(event.target.value === '' ? null : Number(event.target.value))
                }
                className="h-8 w-[92px] text-[12px]"
              >
                <option value="">as created</option>
                {versions
                  .filter((entry) => activeVersion === null || entry.schema_id < activeVersion)
                  .map((entry) => (
                    <option key={entry.schema_id} value={entry.schema_id}>
                      schema {entry.schema_id}
                    </option>
                  ))}
              </Select>
              <span>→ schema {activeVersion}</span>
            </div>
          )}
        </div>

        {mode === 'fields' ? (
          <Panel className="overflow-hidden">
            <PanelHeader
              title={`Schema ${activeVersion}`}
              description={schema ? schemaDescription(schema, activeVersion, versionSpans, timeline) : undefined}
            />
            {!schema ? (
              <div className="space-y-2 p-5">
                <Skeleton className="h-8" />
                <Skeleton className="h-8" />
                <Skeleton className="h-8" />
              </div>
            ) : fields.length === 0 ? (
              <EmptyState title="No matching fields" description="Adjust the filter to see more." />
            ) : fieldLayout === 'tree' ? (
              <SchemaTreeView fields={fields} />
            ) : (
              <SchemaFieldTable fields={fields} timeline={timeline} />
            )}
          </Panel>
        ) : mode === 'timeline' ? (
          <SchemaTimelineView
            fields={liveSchema?.fields ?? []}
            timeline={timelineQuery.data ?? null}
            loading={timelineQuery.isPending}
            error={timelineQuery.error ? timelineQuery.error.message : null}
            onSelectField={(name) => {
              setSelectedVersion(null);
              setSearch(name);
              setShowSuperseded(true);
              setMode('fields');
            }}
          />
        ) : (
          <SchemaDiffView materializationId={materializationId} table={table.name} from={compareFrom} to={activeVersion} />
        )}
      </div>
    </PageBody>
  );
}

/**
 * When a version's changes appear in the table's data: the earliest record that holds a field it
 * added, forked or collapsed. Schema 0 is what discovery sampled, from the topic's oldest records
 * and its newest, so it is the initial schema whatever the dates of its fields. Without dates - not
 * read yet, the table unreadable, or a version none of whose fields hold a value - the time the
 * runtime committed it.
 */
function VersionDate({
  version,
  span,
  timeline,
}: {
  version: CpSchemaVersionRef;
  span: TimeSpan | null;
  timeline: SchemaTimeline | null;
}): ReactNode {
  const committed = version.since ? `Committed to the table ${formatTimestamp(version.since)}` : undefined;
  if (version.schema_id === 0) {
    return (
      <span
        className="text-[11px] text-[var(--color-ink-faint)]"
        title={`Sampled from the topic’s oldest and newest records when the table was created, so it can hold shapes from any part of the topic’s history: each field says when it first held a value.${committed ? ` ${committed}.` : ''}`}
      >
        Initial · sampled
      </span>
    );
  }
  if (span && timeline) {
    return (
      <span
        className="numeric text-[11px] text-[var(--color-ink-faint)]"
        title={`When the data first held a field this version brought, in UTC.${committed ? ` ${committed}.` : ''}`}
      >
        data from {formatSpan(span)}
      </span>
    );
  }
  return (
    <span className="text-[11px] text-[var(--color-ink-faint)]" title={committed}>
      committed <RelativeTime value={version.since} />
    </span>
  );
}

/** `140 fields · data from Apr 15, 2025 UTC · committed Oct 19, 2026, 05:10:03`. */
function schemaDescription(
  schema: CpSchema,
  schemaId: number | null,
  spans: ReadonlyMap<number, TimeSpan>,
  timeline: SchemaTimeline | null,
): string {
  const span = schemaId === null ? undefined : spans.get(schemaId);
  const data = schemaId === 0 ? 'initial, sampled' : span && timeline ? `data from ${formatSpan(span)} UTC` : null;
  return [`${formatCount(schema.field_count)} fields`, data, schema.since ? `committed ${formatTimestamp(schema.since)}` : null]
    .filter((part): part is string => part !== null)
    .join(' · ');
}

/** `+3 added · 1 forked`: what a version changed, from the runtime's own account of the commit. */
function changeSummary(version: CpSchemaVersionRef): string {
  const order = ['added', 'forked', 'collapsed', 'rejected'];
  return order
    .filter((kind) => (version.change_counts[kind] ?? 0) > 0)
    .map((kind) => `${version.change_counts[kind]} ${kind}`)
    .join(' · ');
}

const UNMAPPED_RECENT_MS = 60 * 60 * 1000;

/**
 * What a reader of the schema should know before reading it: fields whose values are now JSON text,
 * values the schema had no room for, and values that went to `_unmapped`.
 */
function SchemaCallouts({ schema }: { schema: CpSchema }): ReactNode {
  const collapsed = schema.fields.filter((field) => field.kind === 'json').length;
  const rejected = schema.changes.filter((change) => change.kind === 'rejected').length;
  const unmappedAt = schema.unmapped?.last_seen_at ? Date.parse(schema.unmapped.last_seen_at) : null;
  const unmappedRecently = unmappedAt !== null && Date.now() - unmappedAt < UNMAPPED_RECENT_MS;
  if (collapsed === 0 && rejected === 0 && !unmappedRecently) return null;
  return (
    <div className="grid gap-2">
      {collapsed > 0 ? (
        <Callout icon={<Braces className="size-4" />} tone="var(--color-phase-paused)">
          {collapsed === 1 ? '1 field is' : `${formatCount(collapsed)} fields are`} kept as JSON text: their values changed
          type too often, or their objects grew too wide, for columns of their own. They read as JSON in the Data tab.
        </Callout>
      ) : null}
      {rejected > 0 ? (
        <Callout icon={<OctagonAlert className="size-4" />} tone="var(--color-phase-failed)">
          The schema is at its field limit: new fields are not added, and their values go to <code>_unmapped</code>. Raise
          the limit under Settings › Schema evolution.
        </Callout>
      ) : null}
      {unmappedRecently ? (
        <Callout icon={<Inbox className="size-4" />} tone="var(--color-phase-pending)">
          Values went to <code>_unmapped</code> in the last hour ({formatCount(schema.unmapped?.total_values ?? 0)} in
          all, last <RelativeTime value={schema.unmapped?.last_seen_at ?? null} />). They are kept as JSON on their rows.
        </Callout>
      ) : null}
    </div>
  );
}

function Callout({ icon, tone, children }: { icon: ReactNode; tone: string; children: ReactNode }): ReactNode {
  return (
    <div
      className="flex items-start gap-3 rounded-lg border px-4 py-3 text-[12px] text-[var(--color-ink-muted)]"
      style={{ borderColor: `color-mix(in srgb, ${tone} 35%, transparent)`, backgroundColor: `color-mix(in srgb, ${tone} 7%, transparent)` }}
    >
      <span className="mt-0.5 shrink-0" style={{ color: tone }}>
        {icon}
      </span>
      <p>{children}</p>
    </div>
  );
}
