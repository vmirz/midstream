import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { config } from '../config.ts';
import type { Caller } from '../controlPlane/client.ts';
import { callerOf, domainOf, permit, permitInHome } from '../auth/guard.ts';
import { ConsoleError, badRequest } from '../errors.ts';
import { consoleSettingDefinition, consoleSettingProblem } from '../settings/catalog.ts';
import { consoleSettings, domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { storage } from '../storage/registry.ts';
import { workerPoolSize } from '../workerPool.ts';
import { parseBody } from './materializations.ts';
import {
  SETTINGS_SCOPES,
  type CpSetting,
  type CpSettingsScope,
  type DeploymentSettingView,
  type RuntimeRollout,
  type SettingValue,
  type SettingView,
  type SettingsHistory,
  type SettingsHistoryEntry,
  type SettingsOverview,
  type SettingsSaveResult,
  type SettingsScope,
  type SettingsScopeView,
} from '../../shared/types.ts';

/** Changes the control plane keeps per scope; see its settings storage. */
const HISTORY_RETAINED = 100;

/** How long a save waits for the settings agent to apply what the API accepted. */
const APPLY_TIMEOUT_MS = 10_000;
const APPLY_POLL_MS = 150;

const settingValue = z.union([z.string().max(2048), z.number().finite(), z.boolean(), z.null()]);

const saveSchema = z.object({
  changes: z
    .array(
      z.object({
        scope: z.enum(SETTINGS_SCOPES),
        values: z.record(z.string().min(1).max(64), settingValue),
        reset: z.array(z.string().min(1).max(64)).max(200),
        baseRevision: z.number().int().min(0),
      }),
    )
    .min(1)
    .max(SETTINGS_SCOPES.length)
    .refine((changes) => new Set(changes.map((change) => change.scope)).size === changes.length, {
      message: 'each scope may appear once',
    }),
});

const historyQuery = z.object({ limit: z.coerce.number().int().min(1).max(HISTORY_RETAINED).optional() });

/** The scopes a settings page edits: the console's own, or a domain's control plane and runtimes. */
type ScopeSet = readonly SettingsScope[];
const CONSOLE_SCOPES: ScopeSet = ['console'];
const DOMAIN_SCOPES: ScopeSet = ['control-plane', 'runtime'];

/**
 * The console's own settings (handbook 20): stored in the settings home's `console` scope, so seeing
 * them needs `settings.view` there and changing them `settings.manage` there.
 */
export async function registerConsoleSettingsRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/settings', { preHandler: permitInHome('settings.view') }, async (request): Promise<SettingsOverview> => {
    const home = domains().home;
    return readOverview(home, callerOf(request, home), CONSOLE_SCOPES);
  });

  app.patch('/api/settings', { preHandler: permitInHome('settings.manage') }, async (request, reply): Promise<SettingsSaveResult> => {
    const home = domains().home;
    return save(home, callerOf(request, home), CONSOLE_SCOPES, request.body, reply);
  });

  app.get('/api/settings/history', { preHandler: permitInHome('settings.view') }, async (request): Promise<SettingsHistory> => {
    const home = domains().home;
    return history(home, callerOf(request, home), CONSOLE_SCOPES, request.query);
  });
}

/** A domain's settings, under `/api/d/<domain>/`: its control plane's and its runtimes'. */
export async function registerSettingsRoutes(app: FastifyInstance): Promise<void> {
  app.get('/settings', { preHandler: permit('settings.view') }, async (request): Promise<SettingsOverview> =>
    readOverview(domainOf(request), callerOf(request), DOMAIN_SCOPES),
  );

  app.patch('/settings', { preHandler: permit('settings.manage') }, async (request, reply): Promise<SettingsSaveResult> =>
    save(domainOf(request), callerOf(request), DOMAIN_SCOPES, request.body, reply),
  );

  app.get('/settings/history', { preHandler: permit('settings.view') }, async (request): Promise<SettingsHistory> =>
    history(domainOf(request), callerOf(request), DOMAIN_SCOPES, request.query),
  );
}

async function save(
  domain: Domain,
  caller: Caller,
  allowed: ScopeSet,
  rawBody: unknown,
  reply: import('fastify').FastifyReply,
): Promise<SettingsSaveResult> {
  const body = parseBody(saveSchema, rawBody);
  const ordered = [...body.changes].sort(
    (left, right) => SETTINGS_SCOPES.indexOf(left.scope) - SETTINGS_SCOPES.indexOf(right.scope),
  );
  for (const change of ordered) {
    if (!allowed.includes(change.scope)) {
      throw badRequest(
        change.scope === 'console'
          ? 'the console\'s own settings are saved on the console\'s Settings page, not a domain\'s'
          : `${scopeLabel([change.scope])} settings are a domain's: save them on that domain's Settings page`,
        { scope: change.scope },
      );
    }
    if (Object.keys(change.values).length === 0 && change.reset.length === 0) {
      throw badRequest(`nothing to change in ${change.scope} settings`, { scope: change.scope });
    }
    if (change.scope === 'console') validateConsoleChange(change.values, change.reset);
  }

  // Control plane first, then runtime, stopping at the first refusal so a save is all or nothing in
  // the common case. Each scope is applied by the settings agent after the API accepts it, so the
  // save waits for that before answering.
  const accepted: { scope: SettingsScope; requestId: string }[] = [];
  for (const change of ordered) {
    try {
      const acceptance = await domain.controlPlane.updateSettings(caller, change.scope, {
        values: change.values,
        reset: change.reset,
        base_revision: change.baseRevision,
        ...(change.scope === 'console' ? { fallbacks: consoleFallbacks(change.values, change.reset) } : {}),
      });
      accepted.push({ scope: change.scope, requestId: acceptance.request_id });
    } catch (cause) {
      if (accepted.length === 0 || !(cause instanceof ConsoleError)) throw withScope(cause, change.scope);
      throw new ConsoleError(
        cause.code,
        `${scopeLabel(accepted.map((item) => item.scope))} settings were saved, but ${scopeLabel([change.scope])} ` +
          `settings were not: ${cause.message}`,
        cause.statusCode,
        { ...cause.details, scope: change.scope, saved: accepted.map((item) => item.scope) },
      );
    }
  }

  const outcomes = await Promise.all(accepted.map((item) => waitForApplied(domain, caller, item.scope, item.requestId)));
  const rejected = outcomes.find((outcome) => outcome.state === 'rejected');
  if (rejected && rejected.state === 'rejected') {
    throw new ConsoleError('settings_rejected', rejected.message, 409, {
      scope: rejected.scope,
      reason: rejected.reason,
    });
  }
  for (const outcome of outcomes) {
    if (outcome.state === 'applied' && outcome.scope === 'console') consoleSettings().install(outcome.document);
  }
  domain.invalidateSystemSettings();
  const pending = outcomes.filter((outcome) => outcome.state === 'pending').map((outcome) => outcome.scope);
  const overview = await readOverview(domain, caller, allowed);
  return reply.code(pending.length > 0 ? 202 : 200).send({ ...overview, pending });
}

async function history(domain: Domain, caller: Caller, scopes: ScopeSet, rawQuery: unknown): Promise<SettingsHistory> {
  const query = historyQuery.safeParse(rawQuery ?? {});
  if (!query.success) throw badRequest('limit must be a whole number from 1 to 100', { field: 'limit' });
  const limit = query.data.limit ?? HISTORY_RETAINED;
  const results = await Promise.allSettled(scopes.map((scope) => domain.controlPlane.settingsHistory(caller, scope, limit)));
  const changes: SettingsHistoryEntry[] = [];
  const errors: SettingsHistory['errors'] = [];
  results.forEach((result, index) => {
    const scope = scopes[index] as SettingsScope;
    if (result.status === 'rejected') {
      errors.push({ scope, message: messageOf(result.reason) });
      return;
    }
    for (const record of result.value.changes) {
      changes.push({
        scope,
        revision: record.revision,
        requestId: record.request_id,
        updatedAt: record.updated_at,
        updatedBy: record.updated_by,
        changes: record.changes.map((item) => ({
          key: item.key,
          before: item.before,
          after: item.after,
          beforeSource: item.before_source,
          afterSource: item.after_source,
        })),
      });
    }
  });
  changes.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt) || right.revision - left.revision);
  return { changes, retainedPerScope: HISTORY_RETAINED, errors };
}

/**
 * A settings page: the console's own scope, read from the settings home, or a domain's control-plane
 * and runtime scopes, with what each deployment fixes.
 */
async function readOverview(domain: Domain, caller: Caller, scopes: ScopeSet): Promise<SettingsOverview> {
  if (scopes.includes('console')) {
    const store = consoleSettings();
    let failure: unknown = null;
    try {
      await store.refresh();
    } catch (cause) {
      failure = cause;
    }
    return {
      generatedAt: new Date().toISOString(),
      controlPlane: { reachable: store.loaded && failure === null, error: failure === null ? null : describeFailure(failure) },
      scopes: { 'control-plane': null, runtime: null, console: store.view() },
      deployment: consoleDeployment(),
      rollout: null,
      historyRetained: HISTORY_RETAINED,
    };
  }
  const [controlPlaneScope, runtimeScope] = await Promise.allSettled([
    domain.controlPlane.settingsScope(caller, 'control-plane'),
    domain.controlPlane.settingsScope(caller, 'runtime'),
  ]);
  const reachable = controlPlaneScope.status === 'fulfilled' && runtimeScope.status === 'fulfilled';
  const failure =
    controlPlaneScope.status === 'rejected'
      ? controlPlaneScope.reason
      : runtimeScope.status === 'rejected'
        ? runtimeScope.reason
        : null;
  const controlPlaneDocument = controlPlaneScope.status === 'fulfilled' ? controlPlaneScope.value : null;
  const runtimeDocument = runtimeScope.status === 'fulfilled' ? runtimeScope.value : null;
  return {
    generatedAt: new Date().toISOString(),
    controlPlane: { reachable, error: failure === null ? null : describeFailure(failure) },
    scopes: {
      'control-plane': controlPlaneDocument ? toScopeView(controlPlaneDocument) : null,
      runtime: runtimeDocument ? toScopeView(runtimeDocument) : null,
      console: null,
    },
    deployment: [...deploymentFrom(controlPlaneDocument), ...deploymentFrom(runtimeDocument), ...domainDeployment(domain)],
    rollout: runtimeDocument?.rollout ? toRollout(runtimeDocument.rollout) : null,
    historyRetained: HISTORY_RETAINED,
  };
}

type ApplyOutcome =
  | { state: 'applied'; scope: SettingsScope; document: CpSettingsScope }
  | { state: 'rejected'; scope: SettingsScope; reason: string; message: string }
  | { state: 'pending'; scope: SettingsScope };

/** Wait until the settings agent has applied, or refused, a change the API accepted. */
async function waitForApplied(domain: Domain, caller: Caller, scope: SettingsScope, requestId: string): Promise<ApplyOutcome> {
  const deadline = Date.now() + APPLY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    try {
      const document = await domain.controlPlane.settingsScope(caller, scope);
      if (document.last_rejection?.request_id === requestId) {
        return {
          state: 'rejected',
          scope,
          reason: document.last_rejection.reason,
          message: document.last_rejection.message,
        };
      }
      if (document.recent_request_ids.includes(requestId)) return { state: 'applied', scope, document };
    } catch {
      // A read that fails mid-save is retried until the deadline; the change itself was accepted.
    }
    await new Promise((resolve) => setTimeout(resolve, APPLY_POLL_MS));
  }
  return { state: 'pending', scope };
}

function validateConsoleChange(values: Record<string, SettingValue>, reset: string[]): void {
  for (const [key, value] of Object.entries(values)) {
    const definition = consoleSettingDefinition(key);
    if (!definition) throw badRequest(`${key} is not a console setting that can be changed`, { field: key, scope: 'console' });
    const problem = consoleSettingProblem(definition, value);
    if (problem) throw badRequest(problem, { field: key, scope: 'console' });
  }
  for (const key of reset) {
    if (!consoleSettingDefinition(key)) {
      throw badRequest(`${key} is not a console setting that can be changed`, { field: key, scope: 'console' });
    }
  }
}

function consoleFallbacks(
  values: Record<string, SettingValue>,
  reset: string[],
): Record<string, { value: SettingValue; source: 'default' | 'deployment' }> {
  const fallbacks: Record<string, { value: SettingValue; source: 'default' | 'deployment' }> = {};
  for (const key of [...Object.keys(values), ...reset]) {
    const fallback = consoleSettings().fallback(key);
    if (fallback) fallbacks[key] = fallback;
  }
  return fallbacks;
}

function toScopeView(document: CpSettingsScope): SettingsScopeView {
  return {
    scope: document.scope,
    revision: document.revision,
    updatedAt: document.updated_at,
    updatedBy: document.updated_by,
    settings: (document.settings ?? []).map(toSettingView),
  };
}

function toSettingView(setting: CpSetting): SettingView {
  return {
    scope: setting.scope,
    key: setting.key,
    section: setting.section,
    label: setting.label,
    description: setting.description,
    kind: setting.kind,
    unit: setting.unit,
    minimum: setting.minimum,
    maximum: setting.maximum,
    options: setting.options ?? null,
    choiceOptions: setting.choice_options ?? null,
    choiceLabels: setting.choice_labels ?? null,
    permission: setting.permission ?? null,
    maxLength: null,
    nullable: setting.nullable,
    advanced: setting.advanced,
    danger: setting.danger,
    effect: setting.effect,
    envVar: setting.env_var,
    value: setting.value,
    source: setting.source,
    defaultValue: setting.default_value,
    deploymentValue: setting.deployment_value,
    deploymentSet: setting.deployment_set,
    updatedAt: setting.updated_at,
    updatedBy: setting.updated_by,
  };
}

function deploymentFrom(document: CpSettingsScope | null): DeploymentSettingView[] {
  return (document?.deployment ?? []).map((entry) => ({
    component: entry.component,
    key: entry.key,
    label: entry.label,
    description: entry.description,
    envVar: entry.env_var,
    value: entry.value,
  }));
}

function toRollout(rollout: NonNullable<CpSettingsScope['rollout']>): RuntimeRollout {
  return {
    revision: rollout.revision,
    total: rollout.total,
    current: rollout.current,
    pending: rollout.pending,
    stopped: rollout.stopped,
    notReporting: rollout.not_reporting,
    unsupported: rollout.unsupported,
    materializations: rollout.materializations.map((item) => ({
      materializationId: item.materialization_id,
      name: item.name,
      state: item.state,
      revision: item.revision,
      members: item.members,
    })),
  };
}

function deploymentEntry(
  key: string,
  label: string,
  value: SettingValue,
  envVar: string | null,
  description: string | null = null,
): DeploymentSettingView {
  return { component: 'console', key, label, value, envVar, description };
}

/** How the console reaches a domain, as its registry says: shown on that domain's Deployment section. */
function domainDeployment(domain: Domain): DeploymentSettingView[] {
  return [
    deploymentEntry('domain', 'Domain', `${domain.name} (${domain.id})`, 'CONSOLE_DOMAINS_FILE', 'Its id is its URL segment, its catalog in SQL and its level of the federated catalog.'),
    deploymentEntry('controlPlaneUrl', 'Control plane', domain.controlPlane.baseUrl, 'CONSOLE_DOMAINS_FILE', 'Where the console reaches this domain: its Service, or its API hostname in another cluster.'),
    deploymentEntry(
      'tailUrl',
      'Live tail',
      domain.tailUrl,
      'CONSOLE_DOMAINS_FILE',
      'The tail app the Data tab reads this domain\'s newest, not yet committed records from. Unset, its tables show committed rows only.',
    ),
    deploymentEntry(
      'serviceToken',
      'Service token',
      domain.serviceToken ? 'configured' : 'not configured',
      `CONSOLE_DOMAIN_${domain.id.toUpperCase()}_SERVICE_TOKEN`,
      'What the console reads this domain\'s live views with. Without it the fleet, details and topics are read as a guest.',
    ),
  ];
}

/** The console's environment-only settings. Credentials are reported as configured or not. */
function consoleDeployment(): DeploymentSettingView[] {
  const entry = deploymentEntry;
  return [
    entry('consoleUrl', 'Console URL', config.consoleUrl, 'CONSOLE_URL', 'Where the sign-in providers send people back to, and the old hostnames redirect to.'),
    entry(
      'domains',
      'Domains',
      domains()
        .all()
        .map((domain) => domain.id)
        .join(', '),
      'CONSOLE_DOMAINS_FILE',
      'Every domain this console serves; each domain\'s Settings page says how it is reached.',
    ),
    entry('settingsHome', 'Settings home', domains().home.id, 'CONSOLE_DOMAINS_FILE', 'The domain whose console scope stores these settings.'),
    entry('version', 'Version', config.version, 'CONSOLE_VERSION'),
    entry('listen', 'Listens on', `${config.host}:${config.port}`, 'HOST, PORT'),
    ...storageDeployment(),
    entry('duckdbMemoryLimit', 'DuckDB memory limit', config.duckdbMemoryLimit, 'CONSOLE_DUCKDB_MEMORY_LIMIT', 'Keep it below the pod’s memory limit.'),
    entry('duckdbThreads', 'DuckDB threads', config.duckdbThreads, 'CONSOLE_DUCKDB_THREADS'),
    entry('runConcurrency', 'Runs scanning at once', config.runConcurrency, 'CONSOLE_RUN_CONCURRENCY'),
    entry('runScanConcurrency', 'Objects one run reads at once', config.runScanConcurrency, 'CONSOLE_RUN_SCAN_CONCURRENCY'),
    entry('workerPool', 'Worker pool', Number(process.env.UV_THREADPOOL_SIZE ?? workerPoolSize), 'UV_THREADPOOL_SIZE', 'Sized at start from the two settings above.'),
    entry('dataDir', 'Data directory', config.dataDir, 'CONSOLE_DATA_DIR'),
    entry('parquetCacheMaxBytes', 'Parquet cache budget', config.parquetCacheMaxBytes, 'CONSOLE_PARQUET_CACHE_MAX_BYTES'),
    entry('corsOrigin', 'CORS origin', config.corsOrigin, 'CONSOLE_CORS_ORIGIN'),
    entry('serveClient', 'Serves the web app', config.serveClient, 'CONSOLE_SERVE_CLIENT'),
    entry('sessionCookieName', 'Session cookies', `${config.sessionCookieName}_<domain>`, 'CONSOLE_SESSION_COOKIE', 'One per domain. Renaming them signs everyone out.'),
    entry(
      'ssoEntra',
      'Microsoft Entra ID',
      config.ssoEntraTenantId && config.ssoEntraClientId && config.ssoEntraClientSecret
        ? `tenant ${config.ssoEntraTenantId}, client ${config.ssoEntraClientId}, secret configured`
        : 'not configured',
      'CONSOLE_SSO_ENTRA_TENANT_ID, CONSOLE_SSO_ENTRA_CLIENT_ID, CONSOLE_SSO_ENTRA_CLIENT_SECRET',
      'This console\'s app registration; each domain accepts it only with the same tenant and client (auth.sso.entra) and its Sign-in setting on.',
    ),
    entry(
      'ssoGithub',
      'GitHub',
      config.ssoGithubClientId && config.ssoGithubClientSecret ? `client ${config.ssoGithubClientId}, secret configured` : 'not configured',
      'CONSOLE_SSO_GITHUB_CLIENT_ID, CONSOLE_SSO_GITHUB_CLIENT_SECRET',
      'This console\'s GitHub OAuth App. Each domain needs its client id and secret too, to check that a token was issued to it.',
    ),
    entry(
      'ssoGoogle',
      'Google',
      config.ssoGoogleClientId && config.ssoGoogleClientSecret
        ? `client ${config.ssoGoogleClientId}, secret configured${config.ssoGoogleHostedDomain ? `, hosted domain ${config.ssoGoogleHostedDomain}` : ''}`
        : 'not configured',
      'CONSOLE_SSO_GOOGLE_CLIENT_ID, CONSOLE_SSO_GOOGLE_CLIENT_SECRET, CONSOLE_SSO_GOOGLE_HOSTED_DOMAIN',
      'This console\'s OAuth client at Google.',
    ),
    entry(
      'ssoOidc',
      config.ssoOidcLabel ? `Single sign-on (${config.ssoOidcLabel})` : 'Other single sign-on',
      config.ssoOidcIssuer && config.ssoOidcClientId && config.ssoOidcClientSecret
        ? `${config.ssoOidcIssuer}, client ${config.ssoOidcClientId}, secret configured`
        : 'not configured',
      'CONSOLE_SSO_OIDC_ISSUER, CONSOLE_SSO_OIDC_CLIENT_ID, CONSOLE_SSO_OIDC_CLIENT_SECRET',
      'An OpenID Connect issuer this console signs people in with: Okta, Keycloak, Auth0, Dex or any other.',
    ),
    entry(
      'cookieSecure',
      'Secure cookie',
      config.cookieSecure,
      'CONSOLE_COOKIE_SECURE',
      'auto marks the session cookie Secure whenever the request arrived over HTTPS.',
    ),
  ];
}

/**
 * Object storage access: every storage profile, which destination prefix it serves and how — read in
 * place by DuckDB (direct) or downloaded first (cached) — and what it reads with, never a value.
 */
function storageDeployment(): DeploymentSettingView[] {
  const profiles = storage().profiles.map((profile) => profile.describe());
  const summary = !config.dataAccessEnabled
    ? 'Off: the Data explorer is turned off'
    : profiles.length === 0
      ? 'None: no object storage is read'
      : profiles.map((profile) => `${profile.name} (${profile.kind}, ${profile.mode})`).join(', ');
  return [
    deploymentEntry(
      'storage',
      'Object storage access',
      summary,
      config.storageFile ? 'CONSOLE_STORAGE_FILE' : 'CONSOLE_STORAGE',
      'One storage profile per destination prefix; a table is read with the profile whose scope covers its location best. Direct: DuckDB reads ' +
        'data files in place with the profile’s credentials, so give them read-only access — a statement’s sandbox does not stop a write ' +
        'to a path it may read, only the SQL guard does. Cached: data files are downloaded first, within the cached-mode budgets.',
    ),
    ...profiles.map((profile) =>
      deploymentEntry(
        `storage.${profile.name}`,
        `Storage profile ${profile.name}`,
        `${profile.scope} · ${profile.mode}`,
        profile.credentialVariables.join(', '),
        [
          profile.kind === 'gcs' ? 'Google Cloud Storage' : profile.kind === 's3' ? 'S3' : 'Azure Blob Storage',
          profile.endpoint ? `at ${profile.endpoint}` : null,
          profile.region ? `region ${profile.region}` : null,
          profile.pathStyle ? 'path-style addressing' : null,
        ]
          .filter(Boolean)
          .join(', ') + `; reads with ${profile.credentials}.`,
      ),
    ),
  ];
}

function withScope(cause: unknown, scope: SettingsScope): unknown {
  if (!(cause instanceof ConsoleError)) return cause;
  return new ConsoleError(cause.code, cause.message, cause.statusCode, { ...cause.details, scope });
}

function describeFailure(cause: unknown): string {
  if (cause instanceof ConsoleError && cause.statusCode === 404) {
    return 'the control plane does not store settings yet; upgrade it to 0.5.0 or later';
  }
  return messageOf(cause);
}

function messageOf(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

function scopeLabel(scopes: SettingsScope[]): string {
  const names: Record<SettingsScope, string> = {
    'control-plane': 'Control-plane',
    runtime: 'Runtime',
    console: 'Console',
  };
  return scopes.map((scope) => names[scope]).join(' and ');
}
