/**
 * The console's own settings as an administrator set them, applied to the running console.
 *
 * Overrides are stored by the control plane — the console keeps no durable state of its own — in
 * the `console` settings scope. This store reads that scope at start, every 30 seconds after, and
 * right after a save, validates each value against the console's catalog, and makes the valid
 * ones the live values behind `config`. Until the control plane has been reached once, the
 * deployment's values apply; afterwards the last overrides read stay in force through an outage.
 */

import {
  applySettingOverrides,
  config,
  currentSettingOverrides,
  deploymentSettings,
  environmentDefault,
  isEnvironmentSet,
} from '../config.ts';
import { SERVICE, controlPlane } from '../controlPlane/client.ts';
import { logger } from '../logger.ts';
import type {
  CpSettingOverride,
  CpSettingsScope,
  SettingSource,
  SettingValue,
  SettingView,
  SettingsScopeView,
} from '../../shared/types.ts';
import { CONSOLE_SETTINGS, consoleSettingDefinition, validConsoleOverrides, type ConsoleSettingDefinition } from './catalog.ts';

const REFRESH_INTERVAL_MS = 30_000;

export class ConsoleSettingsStore {
  private revision = 0;
  private stored: Record<string, CpSettingOverride> = {};
  private updatedAt: string | null = null;
  private updatedBy: string | null = null;
  private loadedAt: number | null = null;
  private inFlight: Promise<CpSettingsScope | null> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;

  /** Whether the stored overrides have been read at least once since the console started. */
  get loaded(): boolean {
    return this.loadedAt !== null;
  }

  start(): void {
    void this.refresh();
    this.timer = setInterval(() => void this.refresh(), REFRESH_INTERVAL_MS);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** Read the stored overrides again and apply them. Concurrent callers share one read. */
  refresh(): Promise<CpSettingsScope | null> {
    this.inFlight ??= (async () => {
      try {
        const scope = await controlPlane.settingsScope(SERVICE, 'console');
        this.install(scope);
        return scope;
      } catch (cause) {
        logger.debug({ err: cause }, 'console settings could not be read; keeping the ones in force');
        return null;
      } finally {
        this.inFlight = null;
      }
    })();
    return this.inFlight;
  }

  /** Apply a console scope read from the control plane. */
  install(scope: CpSettingsScope): void {
    const stored = scope.values ?? {};
    const { valid, rejected } = validConsoleOverrides(
      Object.fromEntries(Object.entries(stored).map(([key, override]) => [key, override.value])),
    );
    if (rejected.length > 0) {
      logger.warn({ settings: rejected }, 'ignoring stored console settings this console does not accept');
    }
    const previousLevel = config.logLevel;
    applySettingOverrides(valid);
    if (config.logLevel !== previousLevel) {
      logger.level = config.logLevel;
      logger.info({ level: config.logLevel }, 'console log level changed');
    }
    this.revision = scope.revision;
    this.stored = stored;
    this.updatedAt = scope.updated_at;
    this.updatedBy = scope.updated_by;
    this.loadedAt = Date.now();
  }

  /** The console scope as the Settings page shows it. */
  view(): SettingsScopeView {
    const applied = currentSettingOverrides() as Record<string, SettingValue | undefined>;
    return {
      scope: 'console',
      revision: this.revision,
      updatedAt: this.updatedAt,
      updatedBy: this.updatedBy,
      settings: CONSOLE_SETTINGS.map((definition) => this.describe(definition, applied)),
    };
  }

  /** What a console setting reads as when it is not overridden, and where that value comes from. */
  fallback(key: string): { value: SettingValue; source: 'default' | 'deployment' } | null {
    const definition = consoleSettingDefinition(key);
    if (!definition) return null;
    const deploymentSet = isEnvironmentSet(definition.envVar);
    return {
      value: deploymentSet ? (deploymentSettings[definition.key] as SettingValue) : defaultValue(definition),
      source: deploymentSet ? 'deployment' : 'default',
    };
  }

  private describe(
    definition: ConsoleSettingDefinition,
    applied: Record<string, SettingValue | undefined>,
  ): SettingView {
    const deploymentSet = isEnvironmentSet(definition.envVar);
    const overridden = applied[definition.key] !== undefined;
    const override = overridden ? this.stored[definition.key] : undefined;
    const source: SettingSource = overridden ? 'override' : deploymentSet ? 'deployment' : 'default';
    return {
      scope: 'console',
      key: definition.key,
      section: definition.section,
      label: definition.label,
      description: definition.description,
      kind: definition.kind,
      unit: definition.unit,
      minimum: definition.minimum ?? null,
      maximum: definition.maximum ?? null,
      options: definition.options ? [...definition.options] : null,
      maxLength: definition.maxLength ?? null,
      nullable: false,
      advanced: definition.advanced ?? false,
      danger: definition.danger ?? false,
      effect: definition.effect ?? null,
      envVar: definition.envVar,
      value: config[definition.key] as SettingValue,
      source,
      defaultValue: defaultValue(definition),
      deploymentValue: deploymentSet ? (deploymentSettings[definition.key] as SettingValue) : null,
      deploymentSet,
      updatedAt: override?.updated_at ?? null,
      updatedBy: override?.updated_by ?? null,
    };
  }
}

function defaultValue(definition: ConsoleSettingDefinition): SettingValue {
  const value = environmentDefault(definition.envVar);
  return value === undefined ? (deploymentSettings[definition.key] as SettingValue) : value;
}

export const consoleSettings = new ConsoleSettingsStore();
