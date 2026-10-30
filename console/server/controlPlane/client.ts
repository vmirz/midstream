import { config } from '../config.ts';
import { ConsoleError, assertSafeIdentifier, upstreamError } from '../errors.ts';
import { parseJson } from '../iceberg/json.ts';
import type {
  CpAccepted,
  CpAccessTokenCreated,
  CpAccessTokenList,
  CpAuditPage,
  CpAuthSession,
  CpDatasetDetail,
  CpDatasetGrantList,
  CpDatasetList,
  CpDetail,
  CpGroupDetail,
  CpGroupList,
  CpIdentityAccepted,
  CpIdentityOutcome,
  CpListResponse,
  CpMaintenanceAccepted,
  CpMySessions,
  CpPermissionCatalog,
  CpRole,
  CpRoleList,
  CpSchema,
  CpSchemaVersions,
  CpSettingsAccepted,
  CpSettingsHistory,
  CpSettingsScope,
  CpSystemSettings,
  CpTable,
  CpTopicList,
  CpUserDetail,
  CpUserList,
  MaintenanceJob,
  MaterializationUpdate,
  PartitionGranularity,
  SettingsScope,
  SettingValue,
  TableRequest,
  CpTokenRevocation,
  CpProtectionReview,
  ProtectionPolicyInput,
} from '../../shared/types.ts';

export interface CreateMaterializationInput {
  /** The topics to read, in their order: each with the dataset its table goes in, and its name. */
  tables: TableRequest[];
  /** Its label; omitted, the materialization goes by its id. */
  name?: string;
  runtime_mode?: string;
  resource_profile?: string | null;
  replicas?: number;
  /** Omitted, on. */
  autoscaling?: boolean;
  skip_tombstones?: boolean;
  /** How its tables are partitioned; omitted, the control plane's default. */
  partition_granularity?: PartitionGranularity;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  /** Omitted, the domain's Protected by default (handbook 22). */
  protection?: ProtectionPolicyInput;
}

/** The settings that can change after creation (`MaterializationUpdate`). */
export type UpdateMaterializationInput = MaterializationUpdate;

/** A change to one settings scope, as the control plane's `PATCH /settings/{scope}` takes it. */
export interface UpdateSettingsInput {
  values: Record<string, SettingValue>;
  reset: string[];
  base_revision: number;
  /** Console scope only: what each changed setting reads as when not overridden, for its history. */
  fallbacks?: Record<string, { value: SettingValue; source: 'default' | 'deployment' }>;
}

/**
 * Who a control-plane request is made as.
 *
 * `service` is the console itself, reading what its live views show; the control plane lets it
 * read and never change. `user` is a person's session (or a guest, with no token), forwarded so the
 * control plane authorizes the person, with their address and browser for its audit log.
 */
export type Caller = { kind: 'service' } | UserCaller;

/** A person's session — or a guest, with no token — and where their request came from. */
export interface UserCaller {
  kind: 'user';
  token: string | null;
  ip?: string | null;
  userAgent?: string | null;
}

export const SERVICE: Caller = { kind: 'service' };

/** A status the control plane chose, and its body: for calls whose non-2xx answers are expected. */
export interface CpRawResponse<T> {
  status: number;
  body: T;
}

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: unknown;
  signal?: AbortSignal;
  /** How long to wait for an answer, when not the console's usual timeout. */
  timeoutMs?: number;
}

const FORWARDED_LIMIT = 256;

function headersFor(caller: Caller, serviceToken: string | null): Record<string, string> {
  if (caller.kind === 'service') {
    return serviceToken ? { authorization: `Bearer ${serviceToken}` } : {};
  }
  const headers: Record<string, string> = {};
  if (caller.token) headers.authorization = `Bearer ${caller.token}`;
  // Forwarded for the audit log only; stripped to a safe charset because they come from a browser.
  if (caller.ip) headers['x-forwarded-for'] = caller.ip.replace(/[^\w.:-]/g, '').slice(0, FORWARDED_LIMIT);
  if (caller.userAgent) {
    headers['x-forwarded-user-agent'] = caller.userAgent.replace(/[^\x20-\x7e]/g, '').slice(0, FORWARDED_LIMIT);
  }
  return headers;
}

/**
 * Thin typed client over the control-plane WebViews.
 *
 * The control plane is the only source of truth; the console never writes to it directly and
 * never caches mutations. Every call names its caller, so a person's change is made with their
 * session and authorized by the control plane, never with the console's own credential.
 */
export class ControlPlaneClient {
  readonly baseUrl: string;
  private readonly serviceToken: string | null;

  /**
   * One domain's control plane: its URL, and the service token the console reads that domain's
   * live views with (handbook 20). Every domain has a client of its own.
   */
  constructor(baseUrl: string, serviceToken: string | null = null) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.serviceToken = serviceToken;
  }

  /** Whether this console holds the domain's service token. */
  get hasServiceToken(): boolean {
    return this.serviceToken !== null;
  }

  /** Call the control plane; a non-2xx answer becomes a `ConsoleError` with its status. */
  async request<T>(caller: Caller, path: string, options: RequestOptions = {}): Promise<T> {
    const { status, payload } = await this.send(caller, path, options);
    if (status < 200 || status >= 300) throw toConsoleError(payload, status);
    return payload as T;
  }

  /** Call the control plane and return whatever it answered, status and all. */
  async raw<T>(caller: Caller, path: string, options: RequestOptions = {}): Promise<CpRawResponse<T>> {
    const { status, payload } = await this.send(caller, path, options);
    return { status, body: payload as T };
  }

  private async send(caller: Caller, path: string, options: RequestOptions): Promise<{ status: number; payload: unknown }> {
    const { method = 'GET', body, signal } = options;
    const timeout = AbortSignal.timeout(options.timeoutMs ?? config.controlPlaneTimeoutMs);
    const composed = signal ? AbortSignal.any([signal, timeout]) : timeout;

    let response: Response;
    try {
      response = await fetch(`${this.baseUrl}${path}`, {
        method,
        signal: composed,
        headers: {
          accept: 'application/json',
          ...headersFor(caller, this.serviceToken),
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
    } catch (cause) {
      throw new ConsoleError(
        'control_plane_unreachable',
        `control plane at ${this.baseUrl} is unreachable`,
        503,
        { reason: cause instanceof Error ? cause.message : String(cause) },
      );
    }

    const text = await response.text();
    const payload = text ? safeJsonParse(text) : null;
    // A gateway whose control plane is not there - a route removed, a cluster down - answers these
    // with a page of its own, never the control plane's JSON error. That is the control plane being
    // unreachable, not a resource it says is missing: a materialization is not deleted because its
    // domain's gateway cannot find the domain.
    if (NOT_THERE.has(response.status) && !isControlPlaneError(payload)) {
      throw new ConsoleError(
        'control_plane_unreachable',
        `control plane at ${this.baseUrl} is unreachable: it answered ${response.status} without the control plane's own error`,
        503,
        { reason: `http_${response.status}` },
      );
    }
    return { status: response.status, payload };
  }

  /** Every topic in the cluster; `refresh` asks the control plane to reload rather than reuse its cache. */
  topics(caller: Caller, refresh = false, signal?: AbortSignal): Promise<CpTopicList> {
    return this.request<CpTopicList>(caller, refresh ? '/topics?refresh=true' : '/topics', { signal });
  }

  list(caller: Caller, limit = 'all', signal?: AbortSignal): Promise<CpListResponse> {
    const query = encodeURIComponent(limit);
    return this.request<CpListResponse>(caller, `/materializations?limit=${query}`, { signal });
  }

  /* ---------------------------------------------------------------------------- datasets */

  /** Every dataset; `q` finds them by name, description or table name. */
  datasets(caller: Caller, q = '', signal?: AbortSignal): Promise<CpDatasetList> {
    const search = q.trim() ? `?q=${encodeURIComponent(q.trim())}` : '';
    return this.request<CpDatasetList>(caller, `/datasets${search}`, { signal });
  }

  /** One dataset and its tables. */
  dataset(caller: Caller, name: string, signal?: AbortSignal): Promise<CpDatasetDetail> {
    return this.request<CpDatasetDetail>(caller, `/datasets/${pathId(name, 'dataset')}`, { signal });
  }

  createDataset(caller: Caller, input: { name: string; description?: string }): Promise<CpIdentityAccepted & { name: string }> {
    return this.request<CpIdentityAccepted & { name: string }>(caller, '/datasets', { method: 'POST', body: input });
  }

  updateDataset(caller: Caller, name: string, input: { description?: string; base_revision?: number }): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/datasets/${pathId(name, 'dataset')}`, { method: 'PATCH', body: input });
  }

  deleteDataset(caller: Caller, name: string): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/datasets/${pathId(name, 'dataset')}`, { method: 'DELETE' });
  }

  /** Who may use a dataset, and the roles that can be granted on it. */
  datasetGrants(caller: Caller, name: string): Promise<CpDatasetGrantList> {
    return this.request<CpDatasetGrantList>(caller, `/datasets/${pathId(name, 'dataset')}/grants`);
  }

  /** Set the roles a person or a group holds on a dataset; none removes their grant. */
  setDatasetGrant(
    caller: Caller,
    name: string,
    principalId: string,
    input: { role_ids: string[]; principal_type?: 'user' | 'group' },
  ): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/datasets/${pathId(name, 'dataset')}/grants/${pathId(principalId, 'principal_id')}`, {
      method: 'PUT',
      body: input,
    });
  }

  detail(caller: Caller, materializationId: string, signal?: AbortSignal): Promise<CpDetail> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpDetail>(caller, `/materializations/${encodeURIComponent(id)}`, { signal });
  }

  /** The current schema of one of a materialization's tables. */
  schema(caller: Caller, materializationId: string, table: string, signal?: AbortSignal): Promise<CpSchema> {
    return this.request<CpSchema>(caller, `${tablePath(materializationId, table)}/schema`, { signal });
  }

  schemaVersions(caller: Caller, materializationId: string, table: string, signal?: AbortSignal): Promise<CpSchemaVersions> {
    return this.request<CpSchemaVersions>(caller, `${tablePath(materializationId, table)}/schema/versions`, { signal });
  }

  schemaVersion(caller: Caller, materializationId: string, table: string, schemaId: number, signal?: AbortSignal): Promise<CpSchema> {
    return this.request<CpSchema>(
      caller,
      `${tablePath(materializationId, table)}/schema/versions/${encodeURIComponent(schemaId)}`,
      { signal },
    );
  }

  /** The protection review: per table, what is masked, since when, and what the current rules would change. */
  protection(caller: Caller, materializationId: string, signal?: AbortSignal): Promise<CpProtectionReview> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpProtectionReview>(caller, `/materializations/${encodeURIComponent(id)}/protection`, { signal });
  }

  /** The control plane's record of one of the materialization's Iceberg tables. */
  table(caller: Caller, materializationId: string, table: string, signal?: AbortSignal): Promise<CpTable> {
    return this.request<CpTable>(caller, tablePath(materializationId, table), { signal });
  }

  /**
   * Ask the maintenance app to run one maintenance job now, on one table or, without one, on every
   * table in turn. Accepted with 202; the app reports the outcome. A reprotect may purge.
   */
  maintain(
    caller: Caller,
    materializationId: string,
    job: MaintenanceJob,
    table: string | null = null,
    options: { purge?: boolean } = {},
  ): Promise<CpMaintenanceAccepted> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpMaintenanceAccepted>(caller, `/materializations/${encodeURIComponent(id)}/maintenance`, {
      method: 'POST',
      body: { job, ...(table === null ? {} : { table }), ...(options.purge === undefined ? {} : { purge: options.purge }) },
    });
  }

  create(caller: Caller, input: CreateMaterializationInput): Promise<CpAccepted> {
    return this.request<CpAccepted>(caller, '/materializations', { method: 'POST', body: input });
  }

  /** What a new materialization takes when its request leaves a setting out. */
  settings(caller: Caller, signal?: AbortSignal): Promise<CpSystemSettings> {
    return this.request<CpSystemSettings>(caller, '/system/settings', { signal });
  }

  /** One scope's settings: every setting with its value and provenance, or the console's overrides. */
  settingsScope(caller: Caller, scope: SettingsScope, signal?: AbortSignal): Promise<CpSettingsScope> {
    return this.request<CpSettingsScope>(caller, `/settings/${encodeURIComponent(scope)}`, { signal });
  }

  /** Change one scope's settings. Accepted with 202; the settings agent applies it shortly after. */
  updateSettings(caller: Caller, scope: SettingsScope, input: UpdateSettingsInput): Promise<CpSettingsAccepted> {
    return this.request<CpSettingsAccepted>(caller, `/settings/${encodeURIComponent(scope)}`, {
      method: 'PATCH',
      body: input,
    });
  }

  /** One scope's recorded changes, newest first. */
  settingsHistory(caller: Caller, scope: SettingsScope, limit = 100, signal?: AbortSignal): Promise<CpSettingsHistory> {
    return this.request<CpSettingsHistory>(
      caller,
      `/settings/${encodeURIComponent(scope)}/history?limit=${encodeURIComponent(limit)}`,
      { signal },
    );
  }

  stop(caller: Caller, materializationId: string): Promise<CpAccepted> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpAccepted>(caller, `/materializations/${encodeURIComponent(id)}/stop`, { method: 'POST' });
  }

  update(caller: Caller, materializationId: string, input: UpdateMaterializationInput): Promise<CpAccepted> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpAccepted>(caller, `/materializations/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      body: input,
    });
  }

  resume(caller: Caller, materializationId: string): Promise<CpAccepted> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpAccepted>(caller, `/materializations/${encodeURIComponent(id)}/resume`, { method: 'POST' });
  }

  remove(caller: Caller, materializationId: string): Promise<CpAccepted> {
    const id = assertSafeIdentifier(materializationId);
    return this.request<CpAccepted>(caller, `/materializations/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  /* ------------------------------------------------------------------------------ sign-in */

  /** Check an email and password. 200 carries the session token; other statuses say why not. */
  login(caller: Caller, input: { email: string; password: string }): Promise<CpRawResponse<CpAuthSession>> {
    return this.raw<CpAuthSession>(caller, '/auth/login', { method: 'POST', body: input });
  }

  /**
   * Hand the domain what a sign-in provider vouched for (handbook 20); it checks it itself. A first
   * sign-in waits there while the account is made, so this waits longer than other calls.
   */
  ssoLogin(caller: Caller, input: { provider: string; credential: Record<string, string>; nonce: string | null }): Promise<CpRawResponse<CpAuthSession>> {
    return this.raw<CpAuthSession>(caller, '/auth/login', { method: 'POST', body: input, timeoutMs: Math.max(config.controlPlaneTimeoutMs, 25_000) });
  }

  /** Who the caller's session belongs to, or a guest; never refuses. */
  session(caller: Caller, signal?: AbortSignal): Promise<CpAuthSession> {
    return this.request<CpAuthSession>(caller, '/auth/session', { signal });
  }

  /** Renew the caller's session; the answer carries a new token. */
  refresh(caller: Caller): Promise<CpAuthSession> {
    return this.request<CpAuthSession>(caller, '/auth/refresh', { method: 'POST' });
  }

  logout(caller: Caller, input: { scope?: string; session_id?: string } = {}): Promise<{ signed_out: boolean }> {
    return this.request<{ signed_out: boolean }>(caller, '/auth/logout', { method: 'POST', body: input });
  }

  changePassword(caller: Caller, input: { current_password: string; new_password: string }): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, '/auth/password', { method: 'POST', body: input });
  }

  mySessions(caller: Caller): Promise<CpMySessions> {
    return this.request<CpMySessions>(caller, '/auth/sessions');
  }

  /** The caller's access tokens, never their secrets. */
  accessTokens(caller: Caller): Promise<CpAccessTokenList> {
    return this.request<CpAccessTokenList>(caller, '/auth/tokens');
  }

  /** Make an access token; the answer carries its text, which is never shown again. */
  createAccessToken(
    caller: Caller,
    input: { name: string; permissions: string[]; expires_in_days: number; datasets?: string[] },
  ): Promise<CpAccessTokenCreated> {
    return this.request<CpAccessTokenCreated>(caller, '/auth/tokens', { method: 'POST', body: input });
  }

  /** Revoke a token made here; the answer carries the revocation, signed, to hand to the other domains. */
  revokeAccessToken(caller: Caller, tokenId: string): Promise<CpIdentityAccepted & { revocation?: CpTokenRevocation }> {
    return this.request<CpIdentityAccepted & { revocation?: CpTokenRevocation }>(caller, `/auth/tokens/${pathId(tokenId, 'token_id')}`, {
      method: 'DELETE',
    });
  }

  /** Revocations this domain holds, from `since` on (handbook 20); read with the service credential. */
  tokenRevocations(caller: Caller, since: string | null): Promise<{ revocations: CpTokenRevocation[] }> {
    const query = since ? `?since=${encodeURIComponent(since)}` : '';
    return this.request<{ revocations: CpTokenRevocation[] }>(caller, `/auth/tokens/revocations${query}`);
  }

  /** Hand this domain revocations another signed; it records each it can verify. */
  applyTokenRevocations(caller: Caller, revocations: CpTokenRevocation[], source: string): Promise<{ request_id: string; accepted: number }> {
    return this.request<{ request_id: string; accepted: number }>(caller, '/auth/tokens/revocations', {
      method: 'POST',
      body: { revocations: revocations.map(({ jti, exp, signature }) => ({ jti, exp, signature })), source },
    });
  }

  /* --------------------------------------------------------------------------- directory */

  users(caller: Caller, query: Record<string, string> = {}): Promise<CpUserList> {
    const search = new URLSearchParams(query).toString();
    return this.request<CpUserList>(caller, `/users${search ? `?${search}` : ''}`);
  }

  user(caller: Caller, userId: string): Promise<CpUserDetail> {
    return this.request<CpUserDetail>(caller, `/users/${pathId(userId, 'user_id')}`);
  }

  createUser(caller: Caller, input: Record<string, unknown>): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, '/users', { method: 'POST', body: input });
  }

  updateUser(caller: Caller, userId: string, input: Record<string, unknown>): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/users/${pathId(userId, 'user_id')}`, { method: 'PATCH', body: input });
  }

  deleteUser(caller: Caller, userId: string): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/users/${pathId(userId, 'user_id')}`, { method: 'DELETE' });
  }

  setUserPassword(caller: Caller, userId: string, input: { password: string }): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/users/${pathId(userId, 'user_id')}/password`, {
      method: 'POST',
      body: input,
    });
  }

  signOutUser(caller: Caller, userId: string, input: { session_id?: string } = {}): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/users/${pathId(userId, 'user_id')}/sign-out`, {
      method: 'POST',
      body: input,
    });
  }

  unlockUser(caller: Caller, userId: string): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/users/${pathId(userId, 'user_id')}/unlock`, { method: 'POST' });
  }

  groups(caller: Caller): Promise<CpGroupList> {
    return this.request<CpGroupList>(caller, '/groups');
  }

  group(caller: Caller, groupId: string): Promise<CpGroupDetail> {
    return this.request<CpGroupDetail>(caller, `/groups/${pathId(groupId, 'group_id')}`);
  }

  createGroup(caller: Caller, input: Record<string, unknown>): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, '/groups', { method: 'POST', body: input });
  }

  updateGroup(caller: Caller, groupId: string, input: Record<string, unknown>): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/groups/${pathId(groupId, 'group_id')}`, { method: 'PATCH', body: input });
  }

  deleteGroup(caller: Caller, groupId: string): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/groups/${pathId(groupId, 'group_id')}`, { method: 'DELETE' });
  }

  roles(caller: Caller): Promise<CpRoleList> {
    return this.request<CpRoleList>(caller, '/roles');
  }

  role(caller: Caller, roleId: string): Promise<CpRole> {
    return this.request<CpRole>(caller, `/roles/${pathId(roleId, 'role_id')}`);
  }

  createRole(caller: Caller, input: Record<string, unknown>): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, '/roles', { method: 'POST', body: input });
  }

  updateRole(caller: Caller, roleId: string, input: Record<string, unknown>): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/roles/${pathId(roleId, 'role_id')}`, { method: 'PATCH', body: input });
  }

  deleteRole(caller: Caller, roleId: string): Promise<CpIdentityAccepted> {
    return this.request<CpIdentityAccepted>(caller, `/roles/${pathId(roleId, 'role_id')}`, { method: 'DELETE' });
  }

  permissions(caller: Caller): Promise<CpPermissionCatalog> {
    return this.request<CpPermissionCatalog>(caller, '/permissions');
  }

  audit(caller: Caller, query: Record<string, string> = {}): Promise<CpAuditPage> {
    const search = new URLSearchParams(query).toString();
    return this.request<CpAuditPage>(caller, `/audit${search ? `?${search}` : ''}`);
  }

  /** Whether a change to users, groups or roles has been applied, refused, or is still pending. */
  identityRequest(caller: Caller, requestId: string): Promise<CpIdentityOutcome> {
    return this.request<CpIdentityOutcome>(caller, `/identity/requests/${pathId(requestId, 'request_id')}`);
  }
}

/** `/materializations/<id>/tables/<table>`, with both checked to be safe in a path. */
function tablePath(materializationId: string, table: string): string {
  const id = assertSafeIdentifier(materializationId);
  const name = assertSafeIdentifier(table, 'table');
  return `/materializations/${encodeURIComponent(id)}/tables/${encodeURIComponent(name)}`;
}

function pathId(value: string, field: string): string {
  return encodeURIComponent(assertSafeIdentifier(value, field));
}

/** The control plane's error as the console's: its own code for sign-in and permission errors. */
function toConsoleError(payload: unknown, status: number): ConsoleError {
  const upstream = extractUpstreamError(payload);
  const message = upstream.message ?? `control plane returned ${status}`;
  if ((status === 401 || status === 403) && (upstream.code === 'unauthenticated' || upstream.code === 'forbidden')) {
    const details = upstream.details && typeof upstream.details === 'object' ? (upstream.details as Record<string, unknown>) : {};
    return new ConsoleError(upstream.code, message, status, details);
  }
  return upstreamError(message, status, {
    upstream_code: upstream.code,
    upstream_details: upstream.details,
  });
}

/** What a gateway answers when the control plane behind it is not there. */
const NOT_THERE = new Set([404, 502, 503, 504]);

/** Whether a body is the control plane's own error: JSON with an `error` object. */
export function isControlPlaneError(payload: unknown): boolean {
  if (!payload || typeof payload !== 'object' || !('error' in payload)) return false;
  const error = (payload as { error: unknown }).error;
  return Boolean(error) && typeof error === 'object' && (error as { code?: unknown }).code !== 'invalid_upstream_payload';
}

/** Snapshot ids are 64-bit, so integers beyond a double's precision are kept as decimal text. */
function safeJsonParse(text: string): unknown {
  try {
    return parseJson(text);
  } catch {
    return { error: { code: 'invalid_upstream_payload', message: text.slice(0, 512) } };
  }
}

function extractUpstreamError(payload: unknown): {
  code?: string;
  message?: string;
  details?: unknown;
} {
  if (payload && typeof payload === 'object' && 'error' in payload) {
    const error = (payload as { error: unknown }).error;
    if (error && typeof error === 'object') {
      const record = error as Record<string, unknown>;
      return {
        code: typeof record.code === 'string' ? record.code : undefined,
        message: typeof record.message === 'string' ? record.message : undefined,
        details: record.details,
      };
    }
  }
  return {};
}

