/**
 * The Settings pages' sections, in the order their navigation lists them (handbook 20).
 *
 * There are two Settings pages. The console's own, at `/settings`, holds this console's settings,
 * stored in the settings home, the domains it serves, your account in each, and every domain's
 * change history. Each domain's, at `/d/<domain>/settings`, holds its control plane's and runtimes'
 * settings and its directory: users, groups, roles and the audit log.
 *
 * Each setting names its section (the control plane names those of its own settings), so a new
 * setting appears on the right page without this file changing; a section on both pages shows each
 * page's own scopes. A section is shown to whoever holds its `permission`; `null` means anyone
 * signed in, which is what the Account section needs.
 */

import {
  Activity,
  Cable,
  Cpu,
  Database,
  GitBranch,
  GitCommitHorizontal,
  Globe,
  History,
  KeyRound,
  ListTree,
  LogIn,
  Radio,
  ScrollText,
  Server,
  Shield,
  ShieldCheck,
  Sparkles,
  Stethoscope,
  Table2,
  Timer,
  Trash2,
  UserRound,
  Users,
  UsersRound,
  type LucideIcon,
} from 'lucide-react';

export type SettingsGroup = 'account' | 'access' | 'platform' | 'console' | 'system';

/** What a section shows: settings with drafts and a save bar, or one of the dedicated pages. */
export type SectionKind = 'settings' | 'deployment' | 'history' | 'account' | 'domains' | 'users' | 'groups' | 'roles' | 'audit';

export interface SettingsSection {
  id: string;
  label: string;
  /** One line under the section's title. */
  summary: string;
  group: SettingsGroup;
  icon: LucideIcon;
  audience: 'admin' | 'personal';
  kind: SectionKind;
  /** Needed to see the section; null for anyone signed in. */
  permission: string | null;
  /**
   * On the console's page, where `permission` must be held: in the settings home, which stores the
   * console's settings (the default), or in any domain, for a section that shows every domain's.
   */
  heldIn?: 'home' | 'any';
}

export const SETTINGS_GROUPS: { id: SettingsGroup; label: string }[] = [
  { id: 'account', label: 'Account' },
  { id: 'access', label: 'Access' },
  { id: 'platform', label: 'Platform' },
  { id: 'console', label: 'Console' },
  { id: 'system', label: 'System' },
];

/** Sections that read and save settings, which the settings overview loads for. */
export const SETTINGS_KINDS: ReadonlySet<SectionKind> = new Set(['settings', 'deployment']);

function section(
  id: string,
  label: string,
  summary: string,
  group: SettingsGroup,
  icon: LucideIcon,
  kind: SectionKind = 'settings',
  permission: string | null = 'settings.view',
  heldIn: 'home' | 'any' = 'home',
): SettingsSection {
  return { id, label, summary, group, icon, audience: permission === null ? 'personal' : 'admin', kind, permission, heldIn };
}

/** The console's own Settings page, at `/settings`. */
export const CONSOLE_SECTIONS: SettingsSection[] = [
  section('account', 'Your account', 'Who you are in each domain, your passwords, your sessions and your access tokens.', 'account', UserRound, 'account', null),
  section('live', 'Live updates', 'How often this console refreshes, and what its charts and rates cover.', 'console', Activity),
  section('data', 'Data explorer', 'Querying materialized data from the Data tab.', 'console', Table2),
  section('live-tail', 'Live tail', 'How the Data tab shows the newest records, before they are committed.', 'console', Radio),
  section('security', 'Security', 'How often the console checks sessions, and how fast it lets anyone try passwords.', 'console', KeyRound),
  section('topics', 'Topics & Kafka', 'How long this console keeps the topic lists it shows.', 'console', ListTree),
  section('diagnostics', 'Diagnostics', 'The console’s logging, and how long it waits for a control plane.', 'console', Stethoscope),
  section('domains', 'Domains', 'Every domain this console serves: whether it can be reached, its version, its signing key.', 'system', Globe, 'domains', null),
  section('deployment', 'Deployment', 'Settings fixed when this console is deployed. Change them in its Helm values.', 'system', Server, 'deployment'),
  section('history', 'Change history', 'Recent changes to every setting of every domain and of this console, with who made them.', 'system', History, 'history', 'settings.view', 'any'),
];

/** A domain's Settings page, at `/d/<domain>/settings`. */
export const DOMAIN_SECTIONS: SettingsSection[] = [
  section('users', 'Users', 'Everyone who can sign in, their groups, and what they may do.', 'access', Users, 'users', 'users.view'),
  section('groups', 'Groups', 'Bundle people into groups, and give groups roles.', 'access', UsersRound, 'groups', 'groups.view'),
  section('roles', 'Roles', 'Reusable sets of permissions, which groups hold.', 'access', ShieldCheck, 'roles', 'roles.view'),
  section('security', 'Security', 'Passwords, sessions, lockouts and how long the audit log is kept.', 'access', KeyRound),
  section('sign-in', 'Sign-in', 'Which providers people sign in with here, what a first sign-in does, and passwords.', 'access', LogIn),
  section('audit', 'Audit log', 'Sign-ins, failed sign-ins, and every change to users, groups and roles.', 'access', ScrollText, 'audit', 'audit.view'),
  section('materializations', 'Materializations', 'Defaults and limits for new materializations.', 'platform', Database),
  section(
    'protection',
    'Data protection',
    'Which kinds of sensitive data new materializations mask before they write them, and how.',
    'platform',
    Shield,
  ),
  section('deletion', 'Deletion & retention', 'What deleting a materialization removes, and how long deleted records are kept.', 'platform', Trash2),
  section('schema', 'Schema evolution', 'How a table’s schema follows its topic: discovery, forks, JSON collapses and limits.', 'platform', GitBranch),
  section('runtimes', 'Runtimes', 'How every runtime batches and seals files. Changes reach running runtimes without a restart.', 'platform', Cpu),
  section('commits', 'Commits', 'How often each runtime commits its files to the table, and how much one commit takes.', 'platform', GitCommitHorizontal),
  section('live-tail', 'Live tail', 'The newest records of each table, served from Kafka within seconds, before they are committed.', 'platform', Radio),
  section('maintenance', 'Compaction & retention', 'Merging small files, expiring old snapshots and removing files no snapshot references.', 'platform', Sparkles),
  section('catalog', 'Iceberg catalog', 'The REST catalog external engines — DuckDB, Spark, Trino, PyIceberg — connect to.', 'platform', Cable),
  section('jobs', 'Background jobs', 'Consumer-lag measurement.', 'platform', Timer),
  section('topics', 'Topics & Kafka', 'How long the control plane keeps topic lists, and how many topics one analysis reads.', 'system', ListTree),
  section('diagnostics', 'Diagnostics', 'What the control plane logs.', 'system', Stethoscope),
  section('deployment', 'Deployment', 'Settings fixed when this domain is deployed, and how the console reaches it.', 'system', Server, 'deployment'),
  section('history', 'Change history', 'Recent changes to this domain’s settings, with who made them and when.', 'system', History, 'history'),
];

/** The sections a person may see: those whose permission they hold, and Account once signed in. */
export function visibleSections(
  sections: SettingsSection[],
  signedIn: boolean,
  can: (permission: string, heldIn: 'home' | 'any') => boolean,
): SettingsSection[] {
  return sections.filter((item) => (item.permission === null ? signedIn : can(item.permission, item.heldIn ?? 'home')));
}

export function findSection(id: string | undefined, sections: SettingsSection[]): SettingsSection | undefined {
  return sections.find((item) => item.id === id);
}

/** Sections holding runtime settings, whose page follows a rollout to the running runtimes. */
export const RUNTIME_SECTIONS: ReadonlySet<string> = new Set(['runtimes', 'commits', 'schema', 'maintenance']);
