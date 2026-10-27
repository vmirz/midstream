/**
 * One domain the console serves (handbook 20): its control plane, read with the domain's service
 * token, the live store following it, its topic catalog, and what the console knows of it.
 *
 * Every domain has its own of each, so nothing one domain answers can be mistaken for another's. The
 * console checks that each control plane is the domain its registry says (`GET /system/version`): one
 * that names another domain is `misconfigured`, and the console serves nothing of it.
 */

import { config } from '../config.ts';
import { ControlPlaneClient, SERVICE } from '../controlPlane/client.ts';
import { LiveStore } from '../live/store.ts';
import { logger } from '../logger.ts';
import { topicCatalogFor, type TopicCatalog } from '../topics/catalog.ts';
import { DATASET_NAME_MAX_LENGTH, LABEL_MAX_LENGTH, NAMESPACE_KEYWORDS, TABLE_NAME_MAX_LENGTH } from '../../shared/tableNames.ts';
import type {
  ConsoleAutoscaling,
  ConsoleDomain,
  ConsoleLiveTail,
  CpDataset,
  CpMaterialization,
  CpSystemSettings,
  MaterializationDefaults,
} from '../../shared/types.ts';
import type { DomainConfig } from './registry.ts';

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

/** How often the console asks each control plane which domain it is, and how new it is. */
const IDENTITY_CHECK_MS = 60_000;
/** How long the defaults a create starts from, and the live tail's switch, are kept. */
const DEFAULTS_TTL_MS = 60_000;
/** How long search keeps a domain's datasets. */
const DATASETS_TTL_MS = 30_000;
/** What a control plane that does not say allows: its own default. */
const DEFAULT_MAX_TOPICS = 20;

/** What a control plane's `GET /system/version` says. */
interface ControlPlaneIdentity {
  version: string | null;
  domain: { id: string; name: string } | null;
  federationKeyId: string | null;
}

export type DomainStatus = ConsoleDomain['status'];

export class Domain {
  readonly id: string;
  readonly name: string;
  readonly local: boolean;
  readonly settingsHome: boolean;
  /** The tail app's base URL; null, and this domain's tables show committed rows only. */
  readonly tailUrl: string | null;
  /** The domain's service token: what the console reads its live views and its tail app with. */
  readonly serviceToken: string | null;
  readonly controlPlane: ControlPlaneClient;
  readonly live: LiveStore;
  readonly topics: TopicCatalog;

  private identity: ControlPlaneIdentity | null = null;
  private identityError: string | null = null;
  private firstCheck: Promise<void> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private systemSettings: { value: CpSystemSettings; fetchedAt: number } | null = null;
  private datasets: { value: CpDataset[]; fetchedAt: number } | null = null;

  constructor(entry: DomainConfig, controlPlane: ControlPlaneClient = new ControlPlaneClient(entry.controlPlaneUrl, entry.serviceToken)) {
    this.id = entry.id;
    this.name = entry.name;
    this.local = entry.local;
    this.settingsHome = entry.settingsHome;
    this.tailUrl = entry.tailUrl;
    this.serviceToken = entry.serviceToken;
    this.controlPlane = controlPlane;
    // Every failure the domain's store and topic list log names the domain.
    const log = logger.child({ domain: entry.id });
    // Its polls see the control plane go and come back first: check at once what it is now.
    this.live = new LiveStore(controlPlane, log, () => void this.checkIdentity());
    this.topics = topicCatalogFor(controlPlane, log);
  }

  /** Begin following the domain: its live store, and which domain its control plane says it is. */
  start(): void {
    void this.ready();
    this.timer = setInterval(() => void this.checkIdentity(), IDENTITY_CHECK_MS);
    this.timer.unref();
    this.live.start();
    // Loaded ahead of the first dialog open; a failure here is retried by the first read.
    this.topics.warm();
    if (!this.serviceToken) {
      logger.warn(
        { domain: this.id },
        `this console has no service token for domain ${this.id}: its live views read the control plane as a guest. ` +
          'Set the domain\'s CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN to its control plane\'s AUTH_SERVICE_TOKEN.',
      );
    }
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.live.stop();
  }

  /** The first identity check, awaited at most once per domain: what routes wait on before serving it. */
  ready(): Promise<void> {
    this.firstCheck ??= this.checkIdentity();
    return this.firstCheck;
  }

  /** Ask the control plane which domain it is, how new it is, and which federation key it holds. */
  async checkIdentity(): Promise<void> {
    try {
      const body = await this.controlPlane.request<{
        version?: string;
        domain?: { id?: string; name?: string } | null;
        federation_key_id?: string | null;
      }>(SERVICE, '/system/version');
      const domain = body.domain && typeof body.domain.id === 'string' ? { id: body.domain.id, name: body.domain.name ?? body.domain.id } : null;
      const previous = this.status;
      this.identity = { version: body.version ?? null, domain, federationKeyId: body.federation_key_id ?? null };
      this.identityError = null;
      if (this.status === 'misconfigured' && previous !== 'misconfigured') {
        logger.error({ domain: this.id, reported: domain?.id ?? null }, this.statusMessage ?? 'misconfigured domain');
      }
    } catch (cause) {
      this.identityError = cause instanceof Error ? cause.message : String(cause);
      logger.debug({ err: cause, domain: this.id }, 'control plane identity unavailable');
    }
  }

  /**
   * Whether the console can work with the domain, as its checks found: unreachable until it first
   * answers, and again while its latest check fails. A control plane last seen as another domain
   * stays misconfigured, reachable or not.
   */
  get status(): DomainStatus {
    if (this.identity === null) return 'unreachable';
    if (this.identity.domain === null || this.identity.domain.id !== this.id) return 'misconfigured';
    if (this.identityError !== null) return 'unreachable';
    if (compatibleControlPlane(this.identity.version) === false) return 'incompatible';
    return 'ok';
  }

  get statusMessage(): string | null {
    switch (this.status) {
      case 'ok':
        return null;
      case 'unreachable':
        return this.identityError
          ? `The control plane of ${this.name} cannot be reached: ${this.identityError}`
          : `The control plane of ${this.name} has not answered yet.`;
      case 'misconfigured': {
        const reported = this.identity?.domain;
        return reported
          ? `The console's registry sends ${this.id} to ${this.controlPlane.baseUrl}, but that control plane is domain ${reported.id}. Nothing of it is shown until the registry is corrected.`
          : `The control plane at ${this.controlPlane.baseUrl} does not say which domain it is. Nothing of it is shown until it does.`;
      }
      case 'incompatible':
        return `${this.name} runs Midstream ${this.identity?.version}; this console needs ${REQUIRED_CONTROL_PLANE_MAJOR}.0 or later.`;
    }
  }

  get version(): string | null {
    return this.identity?.version ?? null;
  }

  get federationKeyId(): string | null {
    return this.identity?.federationKeyId ?? null;
  }

  /** Whether the console serves the domain: not when its control plane says it is another. */
  get served(): boolean {
    return this.status !== 'misconfigured';
  }

  /**
   * The control plane's `GET /system/settings`: the defaults a new materialization starts from, the
   * live tail's switch and autoscaling. Kept a minute; a settings change saved through the console
   * forgets them at once (`invalidateSystemSettings`). A failure is not kept: the last answer is used
   * and the next read asks again. Null for a control plane that cannot be reached.
   */
  async readSystemSettings(): Promise<CpSystemSettings | null> {
    if (this.systemSettings && Date.now() - this.systemSettings.fetchedAt < DEFAULTS_TTL_MS) return this.systemSettings.value;
    try {
      this.systemSettings = { value: await this.controlPlane.settings(SERVICE), fetchedAt: Date.now() };
      return this.systemSettings.value;
    } catch (cause) {
      logger.debug({ err: cause, domain: this.id }, 'control plane settings unavailable');
      return this.systemSettings?.value ?? null;
    }
  }

  invalidateSystemSettings(): void {
    this.systemSettings = null;
  }

  /**
   * Every dataset of the domain, read with its service token and kept half a minute: what search finds
   * datasets by, those with no table yet included. The last list is kept through a failure.
   */
  async readDatasets(): Promise<CpDataset[]> {
    if (this.datasets && Date.now() - this.datasets.fetchedAt < DATASETS_TTL_MS) return this.datasets.value;
    try {
      this.datasets = { value: (await this.controlPlane.datasets(SERVICE)).datasets, fetchedAt: Date.now() };
    } catch (cause) {
      logger.debug({ err: cause, domain: this.id }, 'datasets unavailable for search');
      this.datasets = { value: this.datasets?.value ?? [], fetchedAt: Date.now() };
    }
    return this.datasets.value;
  }

  async materializationDefaults(): Promise<MaterializationDefaults | null> {
    const settings = await this.readSystemSettings();
    return settings ? toDefaults(settings) : null;
  }

  /** Whether the control plane's live tail is on; null when it cannot say. */
  async controlPlaneLiveTailEnabled(): Promise<boolean | null> {
    const settings = await this.readSystemSettings();
    return typeof settings?.live_tail_enabled === 'boolean' ? settings.live_tail_enabled : null;
  }

  /** How the control plane scales runtimes that are far behind; null when it cannot say. */
  async autoscaling(): Promise<ConsoleAutoscaling | null> {
    const settings = await this.readSystemSettings();
    if (typeof settings?.autoscaling_enabled !== 'boolean') return null;
    return {
      enabled: settings.autoscaling_enabled,
      maxReplicas: settings.autoscale_max_replicas ?? settings.max_replicas,
      targetMinutes: settings.autoscale_target_minutes ?? 30,
    };
  }

  /** What the console says of this domain's live tail: configured, and on here and in its control plane. */
  async liveTail(): Promise<ConsoleLiveTail> {
    return liveTailConfig(this.tailUrl, await this.controlPlaneLiveTailEnabled());
  }

  /**
   * Why a run of this materialization reads no live tail, or null when it may: no tail app for the
   * domain, the tail off in this console or in the domain's control plane, or the materialization not
   * running - the control plane tails a running one only.
   */
  async liveTailOff(materialization: Pick<CpMaterialization, 'live_tail' | 'desired_phase'>): Promise<{ reason: string; message: string } | null> {
    if (!this.tailUrl) return { reason: 'not_configured', message: `${this.name} has no live tail configured here (its tailUrl).` };
    if (!config.liveTailEnabled) return { reason: 'console', message: 'Live tail is off in this console (Settings › Live tail).' };
    if ((await this.controlPlaneLiveTailEnabled()) === false) {
      return { reason: 'control_plane', message: `Live tail is off for every materialization in ${this.name} (its Settings › Live tail).` };
    }
    if (materialization.live_tail === false) return { reason: 'materialization', message: 'Live tail is off for this materialization.' };
    if (materialization.desired_phase !== undefined && materialization.desired_phase !== 'running') {
      return { reason: 'stopped', message: 'Live tail follows a materialization only while it runs.' };
    }
    return null;
  }
}

/** What the console says of a domain's live tail: configured, on here and in its control plane. */
export function liveTailConfig(tailUrl: string | null, controlPlaneEnabled: boolean | null): ConsoleLiveTail {
  const configured = Boolean(tailUrl);
  return {
    configured,
    enabled: configured && config.liveTailEnabled && controlPlaneEnabled !== false,
    refreshMs: config.liveTailRefreshMs,
    maxRows: config.liveTailMaxRows,
  };
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
