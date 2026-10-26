import type {
  ApiErrorBody,
  AuthState,
  CpAuditPage,
  CpGroupDetail,
  CpGroupList,
  CpMySessions,
  CpPermissionCatalog,
  CpRole,
  CpRoleList,
  CpUserDetail,
  CpUserList,
  IdentityChangeResult,
  UserFilter,
  UserSort,
  UserStatus,
  ColumnProfile,
  CatalogConnection,
  ConsoleConfig,
  ConsoleFleet,
  ConsoleSession,
  SearchResults,
  CpAccepted,
  CpDatasetDetail,
  CpDatasetGrantList,
  CpDatasetList,
  DatasetChangeResult,
  MaterializationUpdate,
  TableRequest,
  CpAccessTokenCreated,
  CpAccessTokenList,
  CpIdentityAccepted,
  CpMaintenanceAccepted,
  CpSchema,
  CpSchemaVersions,
  DataSummary,
  DetailSnapshot,
  FleetSnapshot,
  HeadPeek,
  RunExportFormat,
  RunExportShape,
  RunPage,
  RunRefresh,
  RunRequest,
  RunSnapshot,
  MaintenanceJob,
  PartitionGranularity,
  SchemaDiff,
  SchemaTimeline,
  ScopeEstimate,
  SettingsChangeInput,
  SettingsHistory,
  SettingsOverview,
  SettingsSaveResult,
  TableView,
  TimeRange,
  TopicCatalogResponse,
} from '@shared/types';

type SessionListener = () => void;
const sessionListeners = new Set<SessionListener>();

/**
 * Hear when a request finds the session gone — expired, ended elsewhere, or the account
 * deactivated — so the page can re-read who is signed in. Returns the unsubscribe function.
 */
export function onSessionLost(listener: SessionListener): () => void {
  sessionListeners.add(listener);
  return () => sessionListeners.delete(listener);
}

export function notifySessionLost(): void {
  for (const listener of sessionListeners) listener();
}

/** Reasons a 401 gives when a session the browser held can no longer be used. */
const LOST_SESSION_REASONS = new Set(['session_expired', 'session_ended', 'invalid_session', 'account_deactivated']);

export class ApiError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly status: number,
    readonly details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    // The session travels in an HttpOnly cookie the page cannot read; this sends it.
    credentials: 'same-origin',
    headers: {
      accept: 'application/json',
      ...(init.body ? { 'content-type': 'application/json' } : {}),
      ...init.headers,
    },
  });

  const text = await response.text();
  if (!response.ok) {
    let body: ApiErrorBody | null = null;
    try {
      body = text ? (JSON.parse(text) as ApiErrorBody) : null;
    } catch {
      body = null;
    }
    const error = new ApiError(
      body?.error.code ?? 'request_failed',
      body?.error.message ?? `request failed with status ${response.status}`,
      response.status,
      body?.error.details ?? {},
    );
    if (response.status === 401 || (response.status === 403 && error.details.reason === 'password_change_required')) {
      const reason = String(error.details.reason ?? '');
      if (LOST_SESSION_REASONS.has(reason) || reason === 'sign_in_required' || reason === 'password_change_required') {
        notifySessionLost();
      }
    }
    throw error;
  }
  return (text ? JSON.parse(text) : null) as T;
}

export interface CreateMaterializationInput {
  /** The topics to read, in their order: each with the dataset its table goes in, and its name. */
  tables: TableRequest[];
  /** Its label; omitted, it goes by its id. */
  name?: string;
  resource_profile?: string;
  replicas?: number;
  /** Omitted, on: the control plane adds members while its runtime is far behind. */
  autoscaling?: boolean;
  skip_tombstones?: boolean;
  /** Omitted, the control plane's default. */
  partition_granularity?: PartitionGranularity;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
}

/** `<domain API>/materializations/<id>/tables/<table>`. */
function tablePath(base: string, id: string, table: string): string {
  return `${base}/materializations/${encodeURIComponent(id)}/tables/${encodeURIComponent(table)}`;
}

/**
 * The console's own API (handbook 20): its config, who the browser is in every domain, every
 * domain's fleet, search, runs - addressed by id, whichever domain they were started in - and the
 * console's own settings, stored in the settings home.
 */
export const api = {
  config: () => request<ConsoleConfig>('/api/config'),
  run: (runId: string) => request<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}`),
  cancelRun: (runId: string) =>
    request<RunSnapshot>(`/api/runs/${encodeURIComponent(runId)}/cancel`, { method: 'POST' }),
  /**
   * The run with the live tail's newest records appended, or a new run in its place (`replaced`):
   * `range` is the run's range resolved again, which a new run takes.
   */
  refreshRun: (runId: string, range: TimeRange | null = null) =>
    request<RunRefresh>(`/api/runs/${encodeURIComponent(runId)}/refresh`, {
      method: 'POST',
      body: JSON.stringify(range ? { range } : {}),
    }),
  runRows: (runId: string, offset: number, limit: number) =>
    request<RunPage>(`/api/runs/${encodeURIComponent(runId)}/rows?offset=${offset}&limit=${limit}`),
  runProfile: (runId: string, column: string) =>
    request<ColumnProfile>(`/api/runs/${encodeURIComponent(runId)}/profile`, {
      method: 'POST',
      body: JSON.stringify({ column }),
    }),
  runExportUrl: (runId: string, format: RunExportFormat, shape: RunExportShape = 'flat') =>
    `/api/runs/${encodeURIComponent(runId)}/export?format=${format}${format === 'json' || format === 'jsonl' ? `&shape=${shape}` : ''}`,
  runStreamUrl: (runId: string) => `/api/stream/runs/${encodeURIComponent(runId)}`,

  /** Who the browser is in every domain, and the sign-in providers the console offers. */
  session: () => request<ConsoleSession>('/api/auth/session'),
  /** Sign out of every domain this browser has a session in. */
  signOutEverywhere: () => request<{ signed_out: boolean; domains: string[] }>('/api/auth/logout', { method: 'POST', body: '{}' }),
  /** Every domain's fleet, as this person may see each. */
  fleet: () => request<ConsoleFleet>('/api/fleet'),
  /** Materializations, tables and datasets across every domain this person may see. */
  search: (q: string, limit = 50) => request<SearchResults>(`/api/search${queryString({ q, limit })}`),
  /** The console's own settings, stored in the settings home. */
  settings: () => request<SettingsOverview>('/api/settings'),
  saveSettings: (changes: SettingsChangeInput[]) =>
    request<SettingsSaveResult>('/api/settings', { method: 'PATCH', body: JSON.stringify({ changes }) }),
  settingsHistory: (limit = 100) => request<SettingsHistory>(`/api/settings/history?limit=${limit}`),
};

/**
 * One domain's API, under `/api/d/<domain>/`: its materializations, datasets, schemas and tables,
 * the runs it starts, its settings, its users, groups and roles, and signing in to it. Every request
 * carries the browser's session in that domain, which the domain authorizes itself.
 */
export function domainApi(domain: string) {
  const base = `/api/d/${encodeURIComponent(domain)}`;
  return {
    domain,
    ...api,
    fleet: () => request<FleetSnapshot>(`${base}/materializations`),
    detail: (id: string) => request<DetailSnapshot>(`${base}/materializations/${encodeURIComponent(id)}`),

    /** Every topic in the cluster. `refresh` asks for a reload, which the server spaces out. */
    topics: (refresh = false) => request<TopicCatalogResponse>(refresh ? `${base}/topics?refresh=1` : `${base}/topics`),

    /** Answers once the new tables' names are claimed; a name taken meanwhile is the error, on its row (`details.field`). */
    create: (input: CreateMaterializationInput) =>
      request<CpAccepted>(`${base}/materializations`, { method: 'POST', body: JSON.stringify(input) }),
    stop: (id: string) =>
      request<CpAccepted>(`${base}/materializations/${encodeURIComponent(id)}/stop`, { method: 'POST' }),
    /** Change what can change after creation: the label, the runtime's replica count, and its topics. */
    update: (id: string, input: MaterializationUpdate) =>
      request<CpAccepted>(`${base}/materializations/${encodeURIComponent(id)}`, {
        method: 'PATCH',
        body: JSON.stringify(input),
      }),
    resume: (id: string) =>
      request<CpAccepted>(`${base}/materializations/${encodeURIComponent(id)}/resume`, { method: 'POST' }),
    remove: (id: string) =>
      request<CpAccepted>(`${base}/materializations/${encodeURIComponent(id)}`, { method: 'DELETE' }),

    schema: (id: string, table: string) => request<CpSchema>(`${tablePath(base, id, table)}/schema`),
    schemaVersions: (id: string, table: string) => request<CpSchemaVersions>(`${tablePath(base, id, table)}/schema/versions`),
    schemaVersion: (id: string, table: string, schemaId: number) =>
      request<CpSchema>(`${tablePath(base, id, table)}/schema/versions/${schemaId}`),
    /** Omitted, `from` is the version before `to`, and `to` the current one; schema 0 diffs against nothing. */
    schemaDiff: (id: string, table: string, from: number | null, to: number | null) => {
      const params = new URLSearchParams();
      if (from !== null) params.set('from', String(from));
      if (to !== null) params.set('to', String(to));
      const query = params.toString();
      return request<SchemaDiff>(`${tablePath(base, id, table)}/schema/diff${query ? `?${query}` : ''}`);
    },
    /** When each field first and last held a value, by the records' Kafka timestamps, from the table's manifests. */
    schemaTimeline: (id: string, table: string) => request<SchemaTimeline>(`${tablePath(base, id, table)}/schema/timeline`),

    /** The Table tab, for one table: snapshots, files by partition, maintenance, properties and how to connect. */
    table: (id: string, table: string) => request<TableView>(tablePath(base, id, table)),
    /** Run one maintenance job now, on one table or every table in turn; accepted at once, reported by the runtime when done. */
    maintain: (id: string, job: MaintenanceJob, table: string | null = null) =>
      request<CpMaintenanceAccepted>(`${base}/materializations/${encodeURIComponent(id)}/maintenance`, {
        method: 'POST',
        body: JSON.stringify(table === null ? { job } : { job, table }),
      }),
    catalogConnection: () => request<CatalogConnection>(`${base}/catalog/connection`),

    /** Every dataset, locked ones included; `q` finds them by name, description or table name. */
    datasets: (q = '') => request<CpDatasetList>(`${base}/datasets${queryString({ q })}`),
    dataset: (name: string) => request<CpDatasetDetail>(`${base}/datasets/${encodeURIComponent(name)}`),
    createDataset: (input: { name: string; description?: string }) =>
      request<DatasetChangeResult>(`${base}/datasets`, { method: 'POST', body: JSON.stringify(input) }),
    updateDataset: (name: string, input: { description: string; base_revision?: number }) =>
      request<DatasetChangeResult>(`${base}/datasets/${encodeURIComponent(name)}`, { method: 'PATCH', body: JSON.stringify(input) }),
    deleteDataset: (name: string) => request<DatasetChangeResult>(`${base}/datasets/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    datasetGrants: (name: string) => request<CpDatasetGrantList>(`${base}/datasets/${encodeURIComponent(name)}/grants`),
    /** The roles a person or a group holds on a dataset; none removes their grant. */
    setDatasetGrant: (name: string, principalId: string, input: { role_ids: string[]; principal_type: 'user' | 'group' }) =>
      request<DatasetChangeResult>(`${base}/datasets/${encodeURIComponent(name)}/grants/${encodeURIComponent(principalId)}`, {
        method: 'PUT',
        body: JSON.stringify(input),
      }),

    /** What one table holds, from its manifests alone; `asOf` names an older snapshot of it. */
    dataSummary: (id: string, table: string, asOf: string | null = null) => {
      const params = new URLSearchParams({ table });
      if (asOf) params.set('asof', asOf);
      return request<DataSummary>(`${base}/materializations/${encodeURIComponent(id)}/data/summary?${params.toString()}`);
    },
    /** What each time range would read of a table, before anything is read. */
    dataEstimate: (id: string, table: string, ranges: TimeRange[], asOf: string | null = null) =>
      request<{ asOf: string; estimates: ScopeEstimate[] }>(
        `${base}/materializations/${encodeURIComponent(id)}/data/estimate`,
        { method: 'POST', body: JSON.stringify({ ranges, table, ...(asOf ? { asOf } : {}) }) },
      ),
    /** Bind a statement without running it; rejects with the error and its position. */
    validateSql: (id: string, sql: string) =>
      request<{ ok: true }>(`${base}/materializations/${encodeURIComponent(id)}/data/validate`, {
        method: 'POST',
        body: JSON.stringify({ sql }),
      }),

    /** Run: the only way the console executes a query. */
    startRun: (id: string, body: RunRequest) =>
      request<RunSnapshot>(`${base}/materializations/${encodeURIComponent(id)}/runs`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    /** The newest records on a table's topic, committed or not, in its columns: a peek while it is backlogged. */
    tableHead: (id: string, table: string, rows = 200) => request<HeadPeek>(`${tablePath(base, id, table)}/head?rows=${rows}`),
    /** Every setting with its value and where it comes from, and the deployment's fixed ones. */
    settings: () => request<SettingsOverview>(`${base}/settings`),
    /** Save edits, one change per scope; answers once they are applied (200) or still applying (202). */
    saveSettings: (changes: SettingsChangeInput[]) =>
      request<SettingsSaveResult>(`${base}/settings`, { method: 'PATCH', body: JSON.stringify({ changes }) }),
    /** Recent changes across every scope, newest first. */
    settingsHistory: (limit = 100) => request<SettingsHistory>(`${base}/settings/history?limit=${limit}`),

    /** Who is signed in, and what they may do; a guest when nobody is. */
    session: () => request<AuthState>(`${base}/auth/session`),
    signIn: (email: string, password: string) =>
      request<AuthState>(`${base}/auth/login`, { method: 'POST', body: JSON.stringify({ email, password }) }),
    signOut: (scope: 'current' | 'others' | 'session' = 'current', sessionId?: string) =>
      request<{ signed_out: boolean }>(`${base}/auth/logout`, {
        method: 'POST',
        body: JSON.stringify({ scope, ...(sessionId ? { session_id: sessionId } : {}) }),
      }),
    /** Change your own password; answers with the session you are signed back in with. */
    changePassword: (currentPassword: string, newPassword: string) =>
      request<{ status: 'applied' | 'pending'; session: AuthState }>(`${base}/auth/password`, {
        method: 'POST',
        body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
      }),
    mySessions: () => request<CpMySessions>(`${base}/auth/sessions`),
    /** Your access tokens for external engines; never their secrets. */
    accessTokens: () => request<CpAccessTokenList>(`${base}/auth/tokens`),
    /** Make an access token, optionally limited to some datasets; the answer's `token` is its text, shown this once. */
    createAccessToken: (input: { name: string; permissions: string[]; expires_in_days: number; datasets?: string[] }) =>
      request<CpAccessTokenCreated>(`${base}/auth/tokens`, { method: 'POST', body: JSON.stringify(input) }),
    revokeAccessToken: (tokenId: string) =>
      request<CpIdentityAccepted>(`${base}/auth/tokens/${encodeURIComponent(tokenId)}`, { method: 'DELETE' }),

    users: (query: { status?: UserFilter; sort?: UserSort; q?: string } = {}) =>
      request<CpUserList>(`${base}/users${queryString(query)}`),
    user: (id: string) => request<CpUserDetail>(`${base}/users/${encodeURIComponent(id)}`),
    createUser: (input: { first_name: string; last_name: string; email: string; password: string; group_ids: string[] }) =>
      request<IdentityChangeResult>(`${base}/users`, { method: 'POST', body: JSON.stringify(input) }),
    updateUser: (
      id: string,
      input: Partial<{ first_name: string; last_name: string; email: string; group_ids: string[]; status: UserStatus; base_revision: number }>,
    ) => request<IdentityChangeResult>(`${base}/users/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(input) }),
    deleteUser: (id: string) => request<IdentityChangeResult>(`${base}/users/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    setUserPassword: (id: string, password: string) =>
      request<IdentityChangeResult>(`${base}/users/${encodeURIComponent(id)}/password`, {
        method: 'POST',
        body: JSON.stringify({ password }),
      }),
    signOutUser: (id: string, sessionId?: string) =>
      request<IdentityChangeResult>(`${base}/users/${encodeURIComponent(id)}/sign-out`, {
        method: 'POST',
        body: JSON.stringify(sessionId ? { session_id: sessionId } : {}),
      }),
    unlockUser: (id: string) => request<IdentityChangeResult>(`${base}/users/${encodeURIComponent(id)}/unlock`, { method: 'POST' }),

    groups: () => request<CpGroupList>(`${base}/groups`),
    group: (id: string) => request<CpGroupDetail>(`${base}/groups/${encodeURIComponent(id)}`),
    createGroup: (input: { name: string; description?: string; role_ids?: string[]; member_ids?: string[] }) =>
      request<IdentityChangeResult>(`${base}/groups`, { method: 'POST', body: JSON.stringify(input) }),
    updateGroup: (
      id: string,
      input: Partial<{
        name: string;
        description: string;
        role_ids: string[];
        add_member_ids: string[];
        remove_member_ids: string[];
        base_revision: number;
      }>,
    ) => request<IdentityChangeResult>(`${base}/groups/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(input) }),
    deleteGroup: (id: string) => request<IdentityChangeResult>(`${base}/groups/${encodeURIComponent(id)}`, { method: 'DELETE' }),

    roles: () => request<CpRoleList>(`${base}/roles`),
    role: (id: string) => request<CpRole>(`${base}/roles/${encodeURIComponent(id)}`),
    createRole: (input: { name: string; description?: string; permissions: string[] }) =>
      request<IdentityChangeResult>(`${base}/roles`, { method: 'POST', body: JSON.stringify(input) }),
    updateRole: (id: string, input: Partial<{ name: string; description: string; permissions: string[]; base_revision: number }>) =>
      request<IdentityChangeResult>(`${base}/roles/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(input) }),
    deleteRole: (id: string) => request<IdentityChangeResult>(`${base}/roles/${encodeURIComponent(id)}`, { method: 'DELETE' }),

    permissions: () => request<CpPermissionCatalog>(`${base}/permissions`),
    audit: (query: { limit?: number; before?: string; action?: string; outcome?: 'success' | 'failure'; q?: string } = {}) =>
      request<CpAuditPage>(`${base}/audit${queryString(query)}`),

  };
}

export type DomainApi = ReturnType<typeof domainApi>;

function queryString(values: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined && value !== '') params.set(key, String(value));
  }
  const text = params.toString();
  return text ? `?${text}` : '';
}

