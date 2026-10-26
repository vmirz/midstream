import type { FastifyInstance } from 'fastify';

import { config, dataAccessMode } from '../config.ts';
import { SERVICE, controlPlane } from '../controlPlane/client.ts';
import { authOf } from '../auth/guard.ts';
import { can } from '../auth/principal.ts';
import { liveStore } from '../live/store.ts';
import { logger } from '../logger.ts';
import { DATASET_NAME_MAX_LENGTH, LABEL_MAX_LENGTH, NAMESPACE_KEYWORDS, TABLE_NAME_MAX_LENGTH } from '../../shared/tableNames.ts';
import type {
  ConsoleAutoscaling,
  ConsoleConfig,
  ConsoleLiveTail,
  CpMaterialization,
  CpSystemSettings,
  MaterializationDefaults,
} from '../../shared/types.ts';

/**
 * The control plane's version, cached because it only changes when that deployment is replaced.
 *
 * Fetched lazily rather than at boot so the console still starts when the control plane is down,
 * and retried on the next request after a failure instead of caching the failure.
 */
let controlPlaneVersion: string | null = null;

async function readControlPlaneVersion(): Promise<string | null> {
  if (controlPlaneVersion !== null) return controlPlaneVersion;
  try {
    const body = await controlPlane.request<{ version?: string }>(SERVICE, '/system/version');
    controlPlaneVersion = body.version ?? null;
  } catch (cause) {
    logger.debug({ err: cause }, 'control plane version unavailable');
    controlPlaneVersion = null;
  }
  return controlPlaneVersion;
}

/**
 * The control plane's `GET /system/settings`: the defaults a new materialization starts from — the
 * destination its tables go under, the namespace and storage its runtime gets — whether its live
 * tail is on, and how runtimes far behind are scaled.
 *
 * They change with a redeploy or a settings override. A change saved through the console forgets
 * them at once (`invalidateMaterializationDefaults`); one made elsewhere shows within the minute
 * they are kept. A failure is not cached, so the next request asks again, and the last answer is
 * used meanwhile. Null for a control plane that cannot be reached or predates the endpoint.
 */
const DEFAULTS_TTL_MS = 60_000;
/** What a control plane that does not say allows: its own default. */
const DEFAULT_MAX_TOPICS = 20;
let systemSettings: { value: CpSystemSettings; fetchedAt: number } | null = null;

/** Forget the cached system settings, so the next read reflects a settings change just saved. */
export function invalidateMaterializationDefaults(): void {
  systemSettings = null;
}

async function readSystemSettings(): Promise<CpSystemSettings | null> {
  if (systemSettings && Date.now() - systemSettings.fetchedAt < DEFAULTS_TTL_MS) return systemSettings.value;
  try {
    systemSettings = { value: await controlPlane.settings(SERVICE), fetchedAt: Date.now() };
    return systemSettings.value;
  } catch (cause) {
    logger.debug({ err: cause }, 'control plane settings unavailable');
    return systemSettings?.value ?? null;
  }
}

async function readMaterializationDefaults(): Promise<MaterializationDefaults | null> {
  const settings = await readSystemSettings();
  return settings ? toDefaults(settings) : null;
}

/** Whether the control plane's live tail is on; null when it cannot say (unreachable, or before 4.1). */
export async function controlPlaneLiveTailEnabled(): Promise<boolean | null> {
  const settings = await readSystemSettings();
  return typeof settings?.live_tail_enabled === 'boolean' ? settings.live_tail_enabled : null;
}

/** How the control plane scales runtimes that are far behind; null when it cannot say (unreachable, or before 4.4). */
export async function controlPlaneAutoscaling(): Promise<ConsoleAutoscaling | null> {
  const settings = await readSystemSettings();
  if (typeof settings?.autoscaling_enabled !== 'boolean') return null;
  return {
    enabled: settings.autoscaling_enabled,
    maxReplicas: settings.autoscale_max_replicas ?? settings.max_replicas,
    targetMinutes: settings.autoscale_target_minutes ?? 30,
  };
}

/** What `GET /api/config` says of the live tail: configured here, on here and in the control plane. */
export function liveTailConfig(controlPlaneEnabled: boolean | null): ConsoleLiveTail {
  const configured = Boolean(config.tailUrl);
  return {
    configured,
    enabled: configured && config.liveTailEnabled && controlPlaneEnabled !== false,
    refreshMs: config.liveTailRefreshMs,
    maxRows: config.liveTailMaxRows,
  };
}

/**
 * Why a run of this materialization reads no live tail, or null when it may: no tail app
 * configured, the tail off in this console or in the control plane, or the materialization not
 * running - the control plane tails a running one only. Every materialization has a live tail; a
 * control plane before 4.3 may still say one is off.
 */
export async function liveTailOff(
  materialization: Pick<CpMaterialization, 'live_tail' | 'desired_phase'>,
): Promise<{ reason: string; message: string } | null> {
  if (!config.tailUrl) return { reason: 'not_configured', message: 'This console has no live tail configured (CONSOLE_TAIL_URL).' };
  if (!config.liveTailEnabled) return { reason: 'console', message: 'Live tail is off in this console (Settings › Live tail).' };
  if ((await controlPlaneLiveTailEnabled()) === false) {
    return { reason: 'control_plane', message: 'Live tail is off for every materialization (Settings › Live tail).' };
  }
  if (materialization.live_tail === false) return { reason: 'materialization', message: 'Live tail is off for this materialization.' };
  if (materialization.desired_phase !== undefined && materialization.desired_phase !== 'running') {
    return { reason: 'stopped', message: 'Live tail follows a materialization only while it runs.' };
  }
  return null;
}

export function toDefaults(settings: CpSystemSettings): MaterializationDefaults {
  return {
    destinationUri: settings.default_destination_uri ?? null,
    namespace: settings.default_namespace,
    replicas: settings.default_replicas,
    maxReplicas: settings.max_replicas,
    storageSize: settings.default_storage_size,
    partitionGranularity: settings.default_partition_granularity ?? 'day',
    labelMaxLength: settings.label_max_length ?? LABEL_MAX_LENGTH,
    datasetNameMaxLength: settings.dataset_name_max_length ?? DATASET_NAME_MAX_LENGTH,
    tableNameMaxLength: settings.table_name_max_length ?? TABLE_NAME_MAX_LENGTH,
    reservedNames: [...settings.reserved_names],
    reservedNamespaceNames: [...(settings.reserved_namespace_names ?? NAMESPACE_KEYWORDS)],
    maxTopics: settings.max_topics ?? DEFAULT_MAX_TOPICS,
  };
}

export async function registerSystemRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/health', async () => {
    const snapshot = liveStore.fleetSnapshot();
    return {
      status: snapshot.connected ? 'ok' : 'degraded',
      controlPlaneConnected: snapshot.connected,
      controlPlaneError: snapshot.controlPlaneError,
      generatedAt: snapshot.generatedAt,
      subscribers: liveStore.subscriberCount,
      version: config.version,
    };
  });

  /**
   * What the web app needs before anything else, signed in or not. What only some people need —
   * the defaults a create starts from, where the control plane is — goes only to them.
   */
  app.get('/api/config', async (request): Promise<ConsoleConfig> => {
    const principal = authOf(request).state.principal;
    const controlPlaneVersion = await readControlPlaneVersion();
    return {
      controlPlaneUrl: can(principal, 'settings.view') ? config.controlPlaneUrl : null,
      dataAccessMode: dataAccessMode(),
      pageRowLimit: config.pageRowLimit,
      exportRowLimit: config.exportRowLimit,
      resultRowLimit: config.resultRowLimit,
      controlPlaneVersion,
      controlPlaneCompatible: compatibleControlPlane(controlPlaneVersion),
      materializationDefaults: can(principal, 'materializations.create') ? await readMaterializationDefaults() : null,
      pollIntervalMs: config.listPollIntervalMs,
      liveTail: liveTailConfig(await controlPlaneLiveTailEnabled()),
      autoscaling: await controlPlaneAutoscaling(),
      version: config.version,
    };
  });
}

/**
 * The oldest control plane this console works with: Midstream 1.0, the first release. Each release
 * candidate reports its version as `1.0.0rc<n>`, which parses to the same major.
 */
export const REQUIRED_CONTROL_PLANE_MAJOR = 1;

/** Whether a control plane version is one this console works with; null when it is unknown. */
export function compatibleControlPlane(version: string | null): boolean | null {
  const major = version === null ? Number.NaN : Number.parseInt(version, 10);
  return Number.isFinite(major) ? major >= REQUIRED_CONTROL_PLANE_MAJOR : null;
}
