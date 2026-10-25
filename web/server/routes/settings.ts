import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { config, dataAccessMode } from '../config.ts';
import { controlPlane, type Caller } from '../controlPlane/client.ts';
import { callerOf, permit } from '../auth/guard.ts';
import { ConsoleError, badRequest } from '../errors.ts';
import { consoleSettingDefinition, consoleSettingProblem } from '../settings/catalog.ts';
import { consoleSettings } from '../settings/store.ts';
import { workerPoolSize } from '../workerPool.ts';
import { invalidateMaterializationDefaults } from './system.ts';
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

export async function registerSettingsRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/settings', { preHandler: permit('settings.view') }, async (request): Promise<SettingsOverview> =>
    readOverview(callerOf(request)),
  );

  app.patch('/api/settings', { preHandler: permit('settings.manage') }, async (request, reply): Promise<SettingsSaveResult> => {
    const caller = callerOf(request);
    const body = parseBody(saveSchema, request.body);
    const ordered = [...body.changes].sort(
      (left, right) => SETTINGS_SCOPES.indexOf(left.scope) - SETTINGS_SCOPES.indexOf(right.scope),
    );
    for (const change of ordered) {
      if (Object.keys(change.values).length === 0 && change.reset.length === 0) {
        throw badRequest(`nothing to change in ${change.scope} settings`, { scope: change.scope });
      }
      if (change.scope === 'console') validateConsoleChange(change.values, change.reset);
    }

    // Control plane first, then runtime, then console, stopping at the first refusal so a save is
    // all or nothing in the common case. Each scope is applied by the settings agent after the
    // API accepts it, so the save waits for that before answering.
    const accepted: { scope: SettingsScope; requestId: string }[] = [];
    for (const change of ordered) {
      try {
        const acceptance = await controlPlane.updateSettings(caller, change.scope, {
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

    const outcomes = await Promise.all(accepted.map((item) => waitForApplied(caller, item.scope, item.requestId)));
    const rejected = outcomes.find((outcome) => outcome.state === 'rejected');
    if (rejected && rejected.state === 'rejected') {
      throw new ConsoleError('settings_rejected', rejected.message, 409, {
        scope: rejected.scope,
        reason: rejected.reason,
      });
    }
    for (const outcome of outcomes) {
      if (outcome.state === 'applied' && outcome.scope === 'console') consoleSettings.install(outcome.document);
    }
    invalidateMaterializationDefaults();
    const pending = outcomes.filter((outcome) => outcome.state === 'pending').map((outcome) => outcome.scope);
    const overview = await readOverview(caller);
    return reply.code(pending.length > 0 ? 202 : 200).send({ ...overview, pending });
  });

  app.get('/api/settings/history', { preHandler: permit('settings.view') }, async (request): Promise<SettingsHistory> => {
    const query = historyQuery.safeParse(request.query ?? {});
    if (!query.success) throw badRequest('limit must be a whole number from 1 to 100', { field: 'limit' });
    const limit = query.data.limit ?? HISTORY_RETAINED;
    const caller = callerOf(request);
    const results = await Promise.allSettled(SETTINGS_SCOPES.map((scope) => controlPlane.settingsHistory(caller, scope, limit)));
    const changes: SettingsHistoryEntry[] = [];
    const errors: SettingsHistory['errors'] = [];
    results.forEach((result, index) => {
      const scope = SETTINGS_SCOPES[index] as SettingsScope;
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
  });
}

async function readOverview(caller: Caller): Promise<SettingsOverview> {
  const [controlPlaneScope, runtimeScope] = await Promise.allSettled([
    controlPlane.settingsScope(caller, 'control-plane'),
    controlPlane.settingsScope(caller, 'runtime'),
    consoleSettings.refresh(),
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
    controlPlane: { reachable: reachable && consoleSettings.loaded, error: failure === null ? null : describeFailure(failure) },
    scopes: {
      'control-plane': controlPlaneDocument ? toScopeView(controlPlaneDocument) : null,
      runtime: runtimeDocument ? toScopeView(runtimeDocument) : null,
      console: consoleSettings.view(),
    },
    deployment: [
      ...deploymentFrom(controlPlaneDocument),
      ...deploymentFrom(runtimeDocument),
      ...consoleDeployment(),
    ],
    rollout: runtimeDocument?.rollout ? toRollout(runtimeDocument.rollout) : null,
    historyRetained: HISTORY_RETAINED,
  };
}

type ApplyOutcome =
  | { state: 'applied'; scope: SettingsScope; document: CpSettingsScope }
  | { state: 'rejected'; scope: SettingsScope; reason: string; message: string }
  | { state: 'pending'; scope: SettingsScope };

/** Wait until the settings agent has applied, or refused, a change the API accepted. */
async function waitForApplied(caller: Caller, scope: SettingsScope, requestId: string): Promise<ApplyOutcome> {
  const deadline = Date.now() + APPLY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    try {
      const document = await controlPlane.settingsScope(caller, scope);
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
    const fallback = consoleSettings.fallback(key);
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

/** The console's environment-only settings. Credentials are reported as configured or not. */
function consoleDeployment(): DeploymentSettingView[] {
  const entry = (
    key: string,
    label: string,
    value: SettingValue,
    envVar: string | null,
    description: string | null = null,
  ): DeploymentSettingView => ({ component: 'console', key, label, value, envVar, description });
  return [
    entry('controlPlaneUrl', 'Control plane', config.controlPlaneUrl, 'CONTROL_PLANE_URL', 'Where the console reads and stores everything, settings included.'),
    entry(
      'tailUrl',
      'Live tail',
      config.tailUrl,
      'CONSOLE_TAIL_URL',
      'The tail app the Data tab reads the newest, not yet committed records from. Unset, the console shows committed rows only.',
    ),
    entry('version', 'Version', config.version, 'CONSOLE_VERSION'),
    entry('listen', 'Listens on', `${config.host}:${config.port}`, 'HOST, PORT'),
    entry(
      'dataAccessMode',
      'Object storage access',
      dataAccessMode(),
      null,
      'direct reads gs:// in place with HMAC credentials; cached downloads objects first; disabled while the Data explorer is off.',
    ),
    entry('hmacCredentials', 'HMAC credentials', config.gcsHmacKeyId && config.gcsHmacSecret ? 'configured' : 'not configured', 'CONSOLE_GCS_HMAC_KEY_ID, CONSOLE_GCS_HMAC_SECRET'),
    entry('duckdbMemoryLimit', 'DuckDB memory limit', config.duckdbMemoryLimit, 'CONSOLE_DUCKDB_MEMORY_LIMIT', 'Keep it below the pod’s memory limit.'),
    entry('duckdbThreads', 'DuckDB threads', config.duckdbThreads, 'CONSOLE_DUCKDB_THREADS'),
    entry('runConcurrency', 'Runs scanning at once', config.runConcurrency, 'CONSOLE_RUN_CONCURRENCY'),
    entry('runScanConcurrency', 'Objects one run reads at once', config.runScanConcurrency, 'CONSOLE_RUN_SCAN_CONCURRENCY'),
    entry('workerPool', 'Worker pool', Number(process.env.UV_THREADPOOL_SIZE ?? workerPoolSize), 'UV_THREADPOOL_SIZE', 'Sized at start from the two settings above.'),
    entry('dataDir', 'Data directory', config.dataDir, 'CONSOLE_DATA_DIR'),
    entry('parquetCacheMaxBytes', 'Parquet cache budget', config.parquetCacheMaxBytes, 'CONSOLE_PARQUET_CACHE_MAX_BYTES'),
    entry('corsOrigin', 'CORS origin', config.corsOrigin, 'CONSOLE_CORS_ORIGIN'),
    entry('serveClient', 'Serves the web app', config.serveClient, 'CONSOLE_SERVE_CLIENT'),
    entry(
      'serviceToken',
      'Service token',
      config.serviceToken ? 'configured' : 'not configured',
      'CONSOLE_SERVICE_TOKEN',
      'What the console reads its live views with. Without it the fleet, details and topics are read as a guest.',
    ),
    entry('sessionCookieName', 'Session cookie', config.sessionCookieName, 'CONSOLE_SESSION_COOKIE', 'Renaming it signs everyone out.'),
    entry(
      'cookieSecure',
      'Secure cookie',
      config.cookieSecure,
      'CONSOLE_COOKIE_SECURE',
      'auto marks the session cookie Secure whenever the request arrived over HTTPS.',
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
