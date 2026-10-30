import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, ChevronDown, CloudOff, RefreshCw } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Navigate, NavLink, useLocation, useNavigate, useParams } from 'react-router-dom';

import { PageBody, PageHeader } from '@/components/AppShell';
import { Button, EmptyState, Panel, PanelHeader, Select, Skeleton } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { ApiError, api, domainApi } from '@/lib/api';
import { useAuth, useConsoleAuth } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { domainPath, useConsoleConfig, useOptionalDomain } from '@/lib/domains';
import { rootQueryClient } from '@/lib/queryClient';
import type { SettingView, SettingsOverview, SettingsScope } from '@shared/types';

import { DeploymentSection } from './DeploymentSection';
import { DomainsSection } from './DomainsSection';
import { HistorySection } from './HistorySection';
import { RuntimeRolloutPanel } from './RuntimeRollout';
import { SettingRow, SettingRowList } from './SettingRow';
import { AccessSectionContent } from '../access/AccessSections';
import {
  CONSOLE_SECTIONS,
  DOMAIN_SECTIONS,
  RUNTIME_SECTIONS,
  SETTINGS_GROUPS,
  SETTINGS_KINDS,
  findSection,
  visibleSections,
  type SettingsSection,
} from './sections';
import {
  SCOPE_LABELS,
  buildChanges,
  conflictNotice,
  draftKey,
  pendingDrafts,
  sectionSettings,
  settingsByKey,
  type Draft,
  type Drafts,
} from './values';

const SCOPE_NOTES: Record<SettingsScope, string> = {
  'control-plane': 'Applies to the control plane.',
  runtime: 'Delivered to every running runtime.',
  console: 'Applies to this console and every other replica of it.',
};

/**
 * Settings: every setting an administrator can change, grouped by what it affects (handbook 20).
 *
 * Two pages share this one. Inside a domain, `/d/<domain>/settings`, it holds that domain's control
 * plane's and runtimes' settings and its directory; at `/settings`, the console's own settings,
 * stored in the settings home, the domains, your account in each and every domain's history.
 *
 * Values come from the deployment's environment unless changed here; a change is stored with a
 * control plane and applies without a restart. Edits stay drafts, across sections, until saved.
 */
export function SettingsPage(): ReactNode {
  const { section: sectionId, itemId } = useParams<{ section?: string; itemId?: string }>();
  const domain = useOptionalDomain();
  const auth = useAuth();
  const consoleAuth = useConsoleAuth();
  const home = useConsoleConfig().data?.settingsHome ?? null;
  const location = useLocation();
  // The console's own settings are stored in the settings home, so it is there that seeing and
  // changing them is decided; inside a domain, the domain decides. Until the console's config names
  // the home, nothing is decided - not even by the domain this browser happens to be signed in to.
  const homeAuth = domain ? auth : home === null ? null : consoleAuth.forDomain(home);
  const can = useCallback(
    (permission: string, heldIn: 'home' | 'any' = 'home') => (heldIn === 'any' ? auth.can(permission) : (homeAuth?.can(permission) ?? false)),
    [auth, homeAuth],
  );
  const sections = useMemo(
    () => visibleSections(domain ? DOMAIN_SECTIONS : CONSOLE_SECTIONS, auth.signedIn, can),
    [domain, auth.signedIn, can],
  );
  const section = findSection(sectionId, sections);
  const settingsApi = useMemo(() => (domain ? domainApi(domain.id) : api), [domain]);
  const base = domain ? domainPath(domain.id, '/settings') : '/settings';
  const canView = can('settings.view');
  const navigate = useNavigate();
  const toast = useToast();
  const queryClient = useQueryClient();
  const [drafts, setDrafts] = useState<Drafts>({});
  const [serverErrors, setServerErrors] = useState<Record<string, string>>({});
  const [conflict, setConflict] = useState<string | null>(null);

  const overview = useQuery({
    queryKey: ['settings'],
    queryFn: () => settingsApi.settings(),
    enabled: canView,
    refetchOnWindowFocus: false,
    // While runtime settings are rolling out, follow it; otherwise the page only changes when saved.
    refetchInterval: (query) =>
      sectionId !== undefined && RUNTIME_SECTIONS.has(sectionId) && hasOutstandingRollout(query.state.data) ? 5_000 : false,
  });
  const settings = useMemo(() => settingsByKey(overview.data), [overview.data]);
  const pending = useMemo(() => pendingDrafts(drafts, settings), [drafts, settings]);
  const readOnly = !overview.data?.controlPlane.reachable || !can('settings.manage');

  const hasEdits = pending.changed.length > 0;
  useEffect(() => {
    if (!hasEdits) return;
    const warn = (event: BeforeUnloadEvent): void => event.preventDefault();
    window.addEventListener('beforeunload', warn);
    // Keep toasts, such as a failed save's, clear of the save bar and its button.
    document.documentElement.style.setProperty('--toast-offset', '4.5rem');
    return () => {
      window.removeEventListener('beforeunload', warn);
      document.documentElement.style.removeProperty('--toast-offset');
    };
  }, [hasEdits]);

  const setDraft = useCallback((setting: SettingView, draft: Draft | undefined) => {
    const key = draftKey(setting);
    setDrafts((current) => {
      const next = { ...current };
      if (draft === undefined) delete next[key];
      else next[key] = draft;
      return next;
    });
    setServerErrors((current) => {
      if (!(key in current)) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  }, []);

  const save = useMutation({
    mutationFn: (current: SettingsOverview) => settingsApi.saveSettings(buildChanges(pending.changed, current)),
    onSuccess: (result) => {
      queryClient.setQueryData(['settings'], result);
      void queryClient.invalidateQueries({ queryKey: ['settings-history'] });
      void rootQueryClient.invalidateQueries({ queryKey: ['settings-history'] });
      // The create dialog shows a domain's defaults, and every page the console's own, from its config.
      void rootQueryClient.invalidateQueries({ queryKey: ['config'] });
      const scopes = new Set(pending.changed.map(([setting]) => setting.scope));
      setDrafts({});
      setServerErrors({});
      setConflict(null);
      if (result.pending.length > 0) {
        toast.success('Settings saved', 'The control plane accepted them and is still applying them.');
        window.setTimeout(() => void queryClient.invalidateQueries({ queryKey: ['settings'] }), 3_000);
      } else {
        toast.success(
          'Settings saved',
          scopes.has('runtime') ? 'Running runtimes pick the change up within seconds.' : 'They apply from now on.',
        );
      }
    },
    onError: (error) => {
      if (!(error instanceof ApiError)) {
        toast.error('Settings were not saved', String(error));
        return;
      }
      const details = error.details as {
        field?: string;
        reason?: string;
        scope?: SettingsScope;
        upstream_details?: { field?: string; reason?: string };
        upstream_code?: string;
      };
      const field = details.field ?? details.upstream_details?.field;
      if (error.status === 409) {
        setConflict(conflictNotice(error.message, details.reason ?? details.upstream_details?.reason));
        return;
      }
      if (field && details.scope) {
        setServerErrors((current) => ({ ...current, [`${details.scope}:${field}`]: error.message }));
      }
      toast.error('Settings were not saved', error.message);
    },
  });

  if (auth.loading) return null;
  if (sections.length === 0) {
    if (auth.signedIn) return <NoSettings domainName={domain?.name ?? null} />;
    return <Navigate to={domain ? `/sign-in/${encodeURIComponent(domain.id)}` : '/sign-in'} replace state={{ from: location }} />;
  }
  if (!section) return <Navigate to={`${base}/${sections[0]!.id}`} replace />;

  const dirtySections = new Set(pending.changed.map(([setting]) => setting.section));
  const usesSettings = SETTINGS_KINDS.has(section.kind);

  return (
    <>
      <PageHeader
        title={domain ? `${domain.name} settings` : 'Settings'}
        subtitle={
          domain
            ? `Configure ${domain.name}'s control plane and runtimes, and who may use it. Changes apply without a restart.`
            : canView
              ? 'Configure this console, see the domains it serves, and manage your account in each.'
              : 'Your account in each domain, and the domains this console serves.'
        }
      />
      <PageBody className="pb-24">
        <div className="grid gap-6 md:grid-cols-[208px_minmax(0,1fr)] md:gap-10">
          <SettingsNav sections={sections} current={section} dirty={dirtySections} base={base} onSelect={(id) => navigate(`${base}/${id}`)} />

          <div className="min-w-0 space-y-5">
            {itemId && !usesSettings ? null : (
              <header>
                <h2 className="text-[15px] font-semibold tracking-tight">{section.label}</h2>
                <p className="mt-1 text-[13px] text-[var(--color-ink-muted)]">{section.summary}</p>
              </header>
            )}

            {section.kind === 'domains' ? <DomainsSection /> : null}
            {section.kind === 'history' ? <HistorySection domain={domain?.id ?? null} /> : null}
            {!usesSettings && section.kind !== 'domains' && section.kind !== 'history' ? (
              <AccessSectionContent section={section} itemId={itemId ?? null} />
            ) : null}

            {usesSettings && overview.data && !overview.data.controlPlane.reachable ? (
              <Banner tone="warning" icon={<CloudOff className="size-4" />}>
                <strong className="font-medium">Settings are read-only.</strong> They are stored with{' '}
                {domain ? `${domain.name}'s control plane` : 'the settings home\'s control plane'}, which cannot be reached:{' '}
                {overview.data.controlPlane.error ?? 'no answer'}.
              </Banner>
            ) : null}

            {usesSettings && conflict ? (
              <Banner
                tone="danger"
                icon={<AlertTriangle className="size-4" />}
                action={
                  <Button
                    size="sm"
                    onClick={() => {
                      setConflict(null);
                      void overview.refetch();
                    }}
                  >
                    <RefreshCw className="size-3.5" />
                    Reload settings
                  </Button>
                }
              >
                {conflict}
              </Banner>
            ) : null}

            {!usesSettings ? null : overview.isLoading ? (
              <Panel className="space-y-4 p-5">
                <Skeleton className="h-4 w-1/3" />
                <Skeleton className="h-4 w-2/3" />
                <Skeleton className="h-4 w-1/2" />
              </Panel>
            ) : overview.error ? (
              <Panel>
                <EmptyState
                  title="Settings could not be loaded"
                  description={overview.error instanceof Error ? overview.error.message : String(overview.error)}
                  action={
                    <Button size="sm" onClick={() => void overview.refetch()}>
                      Retry
                    </Button>
                  }
                />
              </Panel>
            ) : overview.data ? (
              <SectionContent
                section={section}
                overview={overview.data}
                drafts={drafts}
                serverErrors={serverErrors}
                readOnly={readOnly}
                onDraft={setDraft}
                lockedReason={(setting) =>
                  setting.permission && !can(setting.permission)
                    ? `Changing this needs ${permissionLabel(setting.permission)}, besides Manage settings.`
                    : null
                }
              />
            ) : null}
          </div>
        </div>

        {pending.changed.length > 0 ? (
          <div className="pointer-events-none fixed inset-x-0 bottom-5 z-20 flex justify-center px-4 md:pl-[216px]">
            <div className="panel fade-in pointer-events-auto flex w-full max-w-2xl flex-wrap items-center justify-between gap-3 px-4 py-3 shadow-lg">
              <span className="text-[13px]">
                <strong className="font-medium">
                  {pending.changed.length} unsaved {pending.changed.length === 1 ? 'change' : 'changes'}
                </strong>
                {pending.invalid > 0 ? (
                  <span className="ml-2 text-[var(--color-phase-failed)]">
                    {pending.invalid} {pending.invalid === 1 ? 'needs' : 'need'} fixing
                  </span>
                ) : (
                  <span className="ml-2 text-[var(--color-ink-muted)]">in {describeSections(sections, dirtySections)}</span>
                )}
              </span>
              <span className="flex items-center gap-2">
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={save.isPending}
                  onClick={() => {
                    setDrafts({});
                    setServerErrors({});
                  }}
                >
                  Discard
                </Button>
                <Button
                  size="sm"
                  variant="primary"
                  loading={save.isPending}
                  disabled={pending.invalid > 0 || readOnly || !overview.data}
                  onClick={() => overview.data && save.mutate(overview.data)}
                >
                  Save changes
                </Button>
              </span>
            </div>
          </div>
        ) : null}
      </PageBody>
    </>
  );
}

function SectionContent({
  section,
  overview,
  drafts,
  serverErrors,
  readOnly,
  onDraft,
  lockedReason,
}: {
  section: SettingsSection;
  overview: SettingsOverview;
  drafts: Drafts;
  serverErrors: Record<string, string>;
  readOnly: boolean;
  onDraft: (setting: SettingView, draft: Draft | undefined) => void;
  /** Why one setting cannot be changed by this person though the section can (its own permission). */
  lockedReason: (setting: SettingView) => string | null;
}): ReactNode {
  if (section.kind === 'deployment') return <DeploymentSection entries={overview.deployment} />;

  const rows = sectionSettings(overview, section.id);
  const scopes = [...new Set(rows.map((setting) => setting.scope))];
  const showScopeTitles = scopes.length > 1;

  return (
    <div className="space-y-5">
      {scopes.includes('runtime') ? <RuntimeRolloutPanel rollout={overview.rollout} /> : null}
      {rows.length === 0 ? (
        <Panel>
          {readOnly ? (
            <EmptyState
              title="Not available right now"
              description="These settings are stored with the control plane and appear here once it can be reached."
            />
          ) : (
            <EmptyState
              title="Nothing to change here"
              description="Nothing in this section is set here: the control plane did not report any of its settings, or it may be older than this console."
            />
          )}
        </Panel>
      ) : null}
      {scopes.map((scope) => {
        const scoped = rows.filter((setting) => setting.scope === scope);
        const primary = scoped.filter((setting) => !setting.advanced);
        const advanced = scoped.filter((setting) => setting.advanced);
        const renderRow = (setting: SettingView): ReactNode => (
          <SettingRow
            key={draftKey(setting)}
            setting={setting}
            draft={drafts[draftKey(setting)]}
            serverError={serverErrors[draftKey(setting)] ?? null}
            readOnly={readOnly || lockedReason(setting) !== null}
            lockedReason={readOnly ? null : lockedReason(setting)}
            onDraft={(draft) => onDraft(setting, draft)}
          />
        );
        return (
          <div key={scope} className="space-y-3">
            {primary.length > 0 ? (
              <Panel>
                {showScopeTitles ? <PanelHeader title={SCOPE_LABELS[scope]} description={SCOPE_NOTES[scope]} /> : null}
                <SettingRowList>{primary.map(renderRow)}</SettingRowList>
              </Panel>
            ) : null}
            {advanced.length > 0 ? (
              <AdvancedSettings
                count={advanced.length}
                dirty={advanced.some((setting) => drafts[draftKey(setting)] !== undefined)}
                title={showScopeTitles ? `Advanced · ${SCOPE_LABELS[scope]}` : 'Advanced'}
              >
                <SettingRowList>{advanced.map(renderRow)}</SettingRowList>
              </AdvancedSettings>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

/** Rarely changed settings, collapsed until asked for (and while they hold an edit, kept open). */
function AdvancedSettings({
  title,
  count,
  dirty,
  children,
}: {
  title: string;
  count: number;
  dirty: boolean;
  children: ReactNode;
}): ReactNode {
  const [open, setOpen] = useState(dirty);
  const expanded = open || dirty;
  return (
    <Panel>
      <button
        type="button"
        aria-expanded={expanded}
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center justify-between gap-3 px-5 py-3 text-left"
      >
        <span>
          <span className="text-[13px] font-semibold tracking-tight">{title}</span>
          <span className="ml-2 text-[12px] text-[var(--color-ink-faint)]">
            {count} {count === 1 ? 'setting' : 'settings'} you rarely need to change
          </span>
        </span>
        <ChevronDown className={cn('size-4 text-[var(--color-ink-faint)] transition-transform', expanded && 'rotate-180')} />
      </button>
      {expanded ? <div className="border-t border-[var(--color-line)]">{children}</div> : null}
    </Panel>
  );
}

function SettingsNav({
  sections,
  current,
  dirty,
  base,
  onSelect,
}: {
  sections: SettingsSection[];
  current: SettingsSection;
  dirty: Set<string>;
  /** Where the page is: `/settings`, or a domain's `/d/<domain>/settings`. */
  base: string;
  onSelect: (id: string) => void;
}): ReactNode {
  return (
    <>
      <div className="md:hidden">
        <Select aria-label="Settings section" value={current.id} onChange={(event) => onSelect(event.target.value)}>
          {SETTINGS_GROUPS.filter((group) => sections.some((section) => section.group === group.id)).map((group) => (
            <optgroup key={group.id} label={group.label}>
              {sections.filter((section) => section.group === group.id).map((section) => (
                <option key={section.id} value={section.id}>
                  {section.label}
                  {dirty.has(section.id) ? ' •' : ''}
                </option>
              ))}
            </optgroup>
          ))}
        </Select>
      </div>
      <nav aria-label="Settings" className="hidden space-y-5 md:block">
        {SETTINGS_GROUPS.filter((group) => sections.some((section) => section.group === group.id)).map((group) => (
          <div key={group.id}>
            <p className="px-2.5 pb-1.5 text-[11px] font-medium tracking-wide text-[var(--color-ink-faint)] uppercase">
              {group.label}
            </p>
            <ul className="space-y-0.5">
              {sections.filter((section) => section.group === group.id).map((section) => (
                <li key={section.id}>
                  <NavLink
                    to={`${base}/${section.id}`}
                    className={({ isActive }) =>
                      cn(
                        'flex items-center gap-2.5 rounded-[var(--radius-control)] px-2.5 py-1.5 text-[13px] transition-colors',
                        isActive
                          ? 'bg-[var(--color-raised)] font-medium text-[var(--color-ink)]'
                          : 'text-[var(--color-ink-muted)] hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]',
                      )
                    }
                  >
                    <section.icon className="size-4 shrink-0" />
                    <span className="min-w-0 flex-1 truncate">{section.label}</span>
                    {dirty.has(section.id) ? (
                      <span className="size-1.5 rounded-full bg-[var(--color-accent)]" title="Unsaved changes" />
                    ) : null}
                  </NavLink>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </nav>
    </>
  );
}

function Banner({
  tone,
  icon,
  action,
  children,
}: {
  tone: 'warning' | 'danger';
  icon: ReactNode;
  action?: ReactNode;
  children: ReactNode;
}): ReactNode {
  const color = tone === 'danger' ? 'var(--color-phase-failed)' : 'var(--color-phase-paused)';
  return (
    <div
      role="status"
      className="flex flex-wrap items-start gap-3 rounded-[var(--radius-panel)] border px-4 py-3 text-[13px]"
      style={{ borderColor: `color-mix(in srgb, ${color} 35%, transparent)`, background: `color-mix(in srgb, ${color} 8%, transparent)` }}
    >
      <span className="mt-0.5 shrink-0" style={{ color }}>
        {icon}
      </span>
      <p className="min-w-0 flex-1 leading-relaxed">{children}</p>
      {action}
    </div>
  );
}

/** Shown to someone signed in who may see none of a page's sections. */
function NoSettings({ domainName }: { domainName: string | null }): ReactNode {
  return (
    <>
      <PageHeader title={domainName ? `${domainName} settings` : 'Settings'} />
      <PageBody>
        <Panel>
          <EmptyState
            title={domainName ? `You do not have access to ${domainName}'s settings` : 'You do not have access to settings'}
            description="Your groups do not grant it. An administrator can add you to a group that does."
          />
        </Panel>
      </PageBody>
    </>
  );
}

function describeSections(sections: SettingsSection[], ids: Set<string>): string {
  const labels = sections.filter((section) => ids.has(section.id)).map((section) => section.label);
  if (labels.length <= 2) return labels.join(' and ');
  return `${labels.slice(0, -1).join(', ')} and ${labels.at(-1)}`;
}

function hasOutstandingRollout(overview: SettingsOverview | undefined): boolean {
  // Revision 0 too: a runtime may still run settings an earlier installation delivered.
  return (overview?.rollout?.pending ?? 0) > 0;
}

/** A permission's name as the role editor shows it, for the few settings that need one of their own. */
function permissionLabel(permission: string): string {
  return PERMISSION_LABELS[permission] ?? permission;
}

const PERMISSION_LABELS: Record<string, string> = {
  'settings.manage_protection': 'Manage data protection defaults',
};
