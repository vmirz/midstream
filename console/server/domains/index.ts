/**
 * The domains this console serves, made once from the registry (`registry.ts`), and the settings
 * home's store of the console's own settings. A registry that breaks a rule stops the console at
 * start, naming the rule.
 */

import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { ConsoleSettingsStore } from '../settings/store.ts';
import { Domain } from './domain.ts';
import { loadRegistry, type DomainConfig } from './registry.ts';

/** Every domain this console serves, by id, in the registry's order. */
export class Domains {
  private readonly byId: Map<string, Domain>;
  readonly home: Domain;

  constructor(entries: readonly DomainConfig[] | readonly Domain[]) {
    const made = entries.map((entry) => (entry instanceof Domain ? entry : new Domain(entry)));
    this.byId = new Map(made.map((domain) => [domain.id, domain]));
    const home = made.find((domain) => domain.settingsHome);
    if (!home) throw new Error('the registry names no settings home');
    this.home = home;
  }

  all(): Domain[] {
    return [...this.byId.values()];
  }

  /** The domains the console serves: not one whose control plane says it is another. */
  served(): Domain[] {
    return this.all().filter((domain) => domain.served);
  }

  get(id: string): Domain | undefined {
    return this.byId.get(id);
  }

  /** The domain a request names; 404 for one this console does not serve. */
  require(id: string): Domain {
    const domain = this.byId.get(id);
    if (!domain) {
      throw new ConsoleError('unknown_domain', `This console serves no domain ${id}.`, 404, { reason: 'unknown_domain', domain: id });
    }
    return domain;
  }

  get size(): number {
    return this.byId.size;
  }

  start(): void {
    for (const domain of this.all()) domain.start();
  }

  stop(): void {
    for (const domain of this.all()) domain.stop();
  }
}

let current: Domains | null = null;
let settingsStore: ConsoleSettingsStore | null = null;

/** The console's domains, read from the registry the first time they are needed. */
export function domains(): Domains {
  current ??= new Domains(loadRegistry({ file: config.domainsFile, json: config.domainsJson }));
  return current;
}

/** The console's own settings, stored in the settings home's `console` scope. */
export function consoleSettings(): ConsoleSettingsStore {
  settingsStore ??= new ConsoleSettingsStore(domains().home.controlPlane);
  return settingsStore;
}

/** Use these domains instead of the registry's: route tests, which have no control plane to read. */
export function useDomains(replacement: Domains): Domains {
  current = replacement;
  settingsStore = null;
  return replacement;
}
