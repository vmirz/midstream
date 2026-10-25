/**
 * The Settings page's sections, in the order its navigation lists them.
 *
 * Each setting names its section (the control plane names those of its own settings), so a new
 * setting appears on the right page without this file changing. A section is shown to whoever
 * holds its `permission`; `null` means anyone signed in, which is what the Account section needs.
 */

import {
  Activity,
  Cable,
  Cpu,
  Database,
  GitBranch,
  GitCommitHorizontal,
  History,
  KeyRound,
  ListTree,
  Radio,
  ScrollText,
  Server,
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
export type SectionKind = 'settings' | 'deployment' | 'history' | 'account' | 'users' | 'groups' | 'roles' | 'audit';

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
}

export const SETTINGS_GROUPS: { id: SettingsGroup; label: string }[] = [
  { id: 'account', label: 'Account' },
  { id: 'access', label: 'Access' },
  { id: 'platform', label: 'Platform' },
  { id: 'console', label: 'Console' },
  { id: 'system', label: 'System' },
];

/** Sections that read and save settings, which the settings overview loads for. */
export const SETTINGS_KINDS: ReadonlySet<SectionKind> = new Set(['settings', 'deployment', 'history']);

export const SETTINGS_SECTIONS: SettingsSection[] = [
  {
    id: 'account',
    label: 'Your account',
    summary: 'Your profile, your password, and the browsers you are signed in on.',
    group: 'account',
    icon: UserRound,
    audience: 'personal',
    kind: 'account',
    permission: null,
  },
  {
    id: 'users',
    label: 'Users',
    summary: 'Everyone who can sign in, their groups, and what they may do.',
    group: 'access',
    icon: Users,
    audience: 'admin',
    kind: 'users',
    permission: 'users.view',
  },
  {
    id: 'groups',
    label: 'Groups',
    summary: 'Bundle people into groups, and give groups roles.',
    group: 'access',
    icon: UsersRound,
    audience: 'admin',
    kind: 'groups',
    permission: 'groups.view',
  },
  {
    id: 'roles',
    label: 'Roles',
    summary: 'Reusable sets of permissions, which groups hold.',
    group: 'access',
    icon: ShieldCheck,
    audience: 'admin',
    kind: 'roles',
    permission: 'roles.view',
  },
  {
    id: 'security',
    label: 'Security',
    summary: 'Passwords, sessions, lockouts and how long the audit log is kept.',
    group: 'access',
    icon: KeyRound,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'audit',
    label: 'Audit log',
    summary: 'Sign-ins, failed sign-ins, and every change to users, groups and roles.',
    group: 'access',
    icon: ScrollText,
    audience: 'admin',
    kind: 'audit',
    permission: 'audit.view',
  },
  {
    id: 'materializations',
    label: 'Materializations',
    summary: 'Defaults and limits for new materializations.',
    group: 'platform',
    icon: Database,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'deletion',
    label: 'Deletion & retention',
    summary: 'What deleting a materialization removes, and how long deleted records are kept.',
    group: 'platform',
    icon: Trash2,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'schema',
    label: 'Schema evolution',
    summary: 'How a table’s schema follows its topic: discovery, forks, JSON collapses and limits.',
    group: 'platform',
    icon: GitBranch,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'runtimes',
    label: 'Runtimes',
    summary: 'How every runtime batches and seals files. Changes reach running runtimes without a restart.',
    group: 'platform',
    icon: Cpu,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'commits',
    label: 'Commits',
    summary: 'How often each runtime commits its files to the table, and how much one commit takes.',
    group: 'platform',
    icon: GitCommitHorizontal,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'live-tail',
    label: 'Live tail',
    summary: 'The newest records of each table, served from Kafka within seconds, before they are committed.',
    group: 'platform',
    icon: Radio,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'maintenance',
    label: 'Compaction & retention',
    summary: 'Merging small files, expiring old snapshots and removing files no snapshot references.',
    group: 'platform',
    icon: Sparkles,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'catalog',
    label: 'Iceberg catalog',
    summary: 'The REST catalog external engines — DuckDB, Spark, Trino, PyIceberg — connect to.',
    group: 'platform',
    icon: Cable,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'jobs',
    label: 'Background jobs',
    summary: 'Consumer-lag measurement.',
    group: 'platform',
    icon: Timer,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'live',
    label: 'Live updates',
    summary: 'How often this console refreshes, and what its charts and rates cover.',
    group: 'console',
    icon: Activity,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'data',
    label: 'Data explorer',
    summary: 'Querying materialized data from the Data tab.',
    group: 'console',
    icon: Table2,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'topics',
    label: 'Topics & Kafka',
    summary: 'How long topic lists are cached and analysed.',
    group: 'system',
    icon: ListTree,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'diagnostics',
    label: 'Diagnostics',
    summary: 'Logging, timeouts and attribution.',
    group: 'system',
    icon: Stethoscope,
    audience: 'admin',
    kind: 'settings',
    permission: 'settings.view',
  },
  {
    id: 'deployment',
    label: 'Deployment',
    summary: 'Settings fixed when each component is deployed. Change them in its manifests.',
    group: 'system',
    icon: Server,
    audience: 'admin',
    kind: 'deployment',
    permission: 'settings.view',
  },
  {
    id: 'history',
    label: 'Change history',
    summary: 'Recent changes to every setting, with who made them and when.',
    group: 'system',
    icon: History,
    audience: 'admin',
    kind: 'history',
    permission: 'settings.view',
  },
];

/** The sections a person may see: those whose permission they hold, and Account once signed in. */
export function visibleSections(signedIn: boolean, can: (permission: string) => boolean): SettingsSection[] {
  return SETTINGS_SECTIONS.filter((section) => (section.permission === null ? signedIn : can(section.permission)));
}

export function findSection(id: string | undefined, sections: SettingsSection[] = SETTINGS_SECTIONS): SettingsSection | undefined {
  return sections.find((section) => section.id === id);
}

/** Sections holding runtime settings, whose page follows a rollout to the running runtimes. */
export const RUNTIME_SECTIONS: ReadonlySet<string> = new Set(['runtimes', 'commits', 'schema', 'maintenance']);

/** Who each scope's settings configure, as a heading reads. */
export const SCOPE_LABELS = {
  'control-plane': 'Control plane',
  runtime: 'Runtimes',
  console: 'This console',
} as const;
