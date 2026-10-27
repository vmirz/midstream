/**
 * The domains this console serves, and how to reach each (handbook 20).
 *
 * The chart renders them into a JSON file (`CONSOLE_DOMAINS_FILE`); development may set the same JSON
 * inline (`CONSOLE_DOMAINS`). Each domain's service token comes from its own variable,
 * `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`, never from the file. With neither, the console serves one
 * domain, `local`, at http://localhost:6065: a control plane port-forwarded to a laptop.
 *
 * The registry is checked as the chart checks it, so a console started by hand refuses what the chart
 * would: an id that cannot be a domain's, the same id twice, anything but exactly one settings home.
 */

import { readFileSync } from 'node:fs';

import { domainIdProblem } from '../../shared/tableNames.ts';

/** One domain as the console reaches it. */
export interface DomainConfig {
  id: string;
  name: string;
  /** This release's own control plane, reached through its Service. */
  local: boolean;
  /** The one domain storing the console's own settings. */
  settingsHome: boolean;
  controlPlaneUrl: string;
  /** Its tail app; null, and the domain's tables show committed rows only. */
  tailUrl: string | null;
  /** What the console reads the domain's live views with; null, and it reads them as a guest. */
  serviceToken: string | null;
}

/** The development default: one control plane on this machine. */
export const DEVELOPMENT_DOMAIN: Omit<DomainConfig, 'serviceToken'> = {
  id: 'local',
  name: 'Local',
  local: true,
  settingsHome: true,
  controlPlaneUrl: 'http://localhost:6065',
  tailUrl: null,
};

export class RegistryError extends Error {}

/** The variable holding a domain's service token. */
export function serviceTokenVariable(id: string): string {
  return `CONSOLE_DOMAIN_${id.toUpperCase()}_SERVICE_TOKEN`;
}

/**
 * Read the registry: the file, else the inline JSON, else the development default; then every rule.
 * `env` is the process's environment, and is where each domain's token is read from.
 */
export function loadRegistry(
  source: { file?: string | null; json?: string | null },
  env: Record<string, string | undefined> = process.env,
): DomainConfig[] {
  let raw: unknown;
  if (source.file) {
    let text: string;
    try {
      text = readFileSync(source.file, 'utf8');
    } catch (cause) {
      throw new RegistryError(`cannot read the domains file ${source.file}: ${cause instanceof Error ? cause.message : String(cause)}`);
    }
    raw = parse(text, source.file);
  } else if (source.json) {
    raw = parse(source.json, 'CONSOLE_DOMAINS');
  } else {
    raw = [DEVELOPMENT_DOMAIN];
  }
  return validateRegistry(raw, env);
}

function parse(text: string, where: string): unknown {
  try {
    return JSON.parse(text);
  } catch (cause) {
    throw new RegistryError(`${where} is not JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
}

/** Every rule of the registry, with each domain's token read from `env`. */
export function validateRegistry(raw: unknown, env: Record<string, string | undefined> = process.env): DomainConfig[] {
  const entries = Array.isArray(raw) ? raw : (raw as { domains?: unknown } | null)?.domains;
  if (!Array.isArray(entries) || entries.length === 0) throw new RegistryError('the registry names no domain');
  const seen = new Set<string>();
  const domains = entries.map((entry, index): DomainConfig => {
    const where = `domain ${index + 1}`;
    if (!entry || typeof entry !== 'object') throw new RegistryError(`${where} is not an object`);
    const value = entry as Record<string, unknown>;
    const id = typeof value.id === 'string' ? value.id.trim() : '';
    const problem = domainIdProblem(id);
    if (problem) throw new RegistryError(`${where}: ${problem}`);
    if (seen.has(id)) throw new RegistryError(`${where}: domain ${id} is named twice`);
    seen.add(id);
    const controlPlaneUrl = url(value.controlPlaneUrl, `${where} (${id}): controlPlaneUrl`, true) as string;
    const token = (env[serviceTokenVariable(id)] ?? '').trim();
    return {
      id,
      name: typeof value.name === 'string' && value.name.trim() ? value.name.trim() : id,
      local: value.local === true,
      settingsHome: value.settingsHome === true,
      controlPlaneUrl,
      tailUrl: url(value.tailUrl, `${where} (${id}): tailUrl`, false),
      serviceToken: token || null,
    };
  });
  const homes = domains.filter((domain) => domain.settingsHome);
  if (homes.length !== 1) {
    throw new RegistryError(
      `exactly one domain must be the settings home, where the console's own settings are stored; ${homes.length} are`,
    );
  }
  return domains;
}

function url(value: unknown, what: string, required: boolean): string | null {
  if (value === undefined || value === null || value === '') {
    if (required) throw new RegistryError(`${what} is required`);
    return null;
  }
  if (typeof value !== 'string') throw new RegistryError(`${what} must be a URL`);
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new RegistryError(`${what} is not a URL: ${value}`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') throw new RegistryError(`${what} must be http or https`);
  return value.replace(/\/+$/, '');
}
