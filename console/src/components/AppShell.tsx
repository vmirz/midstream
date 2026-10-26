import { Activity, Check, ChevronsUpDown, Database, FolderOpen, Info, Layers, LogIn, LogOut, Moon, PanelLeftClose, PanelLeftOpen, Search, Settings, Sun, UserRound } from 'lucide-react';
import { useCallback, useState, type ReactNode } from 'react';
import { NavLink, useLocation, useNavigate } from 'react-router-dom';

import { Avatar } from './Avatar';
import { Dialog } from './ui/Dialog';
import { Popover } from './ui/Popover';
import { Button } from './ui/primitives';
import { cn } from '@/lib/cn';
import { useConsoleAuth, type Auth } from '@/lib/auth';
import { sectionOf, sectionPath, useConsoleConfig, useScopeDomainId, type ShellSection } from '@/lib/domains';
import { formatRelativeTime } from '@/lib/format';
import { useTicker, type LiveStatus } from '@/lib/live';
import type { ConsoleDomain } from '@shared/types';

/** The pages people work in, in every domain or in one (handbook 20). */
const NAV_ITEMS: Array<{ section: ShellSection; label: string; icon: typeof Activity; permission: string | null }> = [
  { section: 'overview', label: 'Overview', icon: Activity, permission: 'materializations.view' },
  { section: 'materializations', label: 'Materializations', icon: Database, permission: 'materializations.view' },
  { section: 'datasets', label: 'Datasets', icon: FolderOpen, permission: 'datasets.view' },
  { section: 'search', label: 'Search', icon: Search, permission: null },
];

/** Pinned to the bottom of the sidebar, apart from the pages people work in. */
const SETTINGS_ITEM = { section: 'settings' as const, label: 'Settings', icon: Settings };

/** Settings holds everyone's own account, so it is there for anyone signed in. */
function canOpenSettings(auth: Auth): boolean {
  return auth.signedIn || auth.can('settings.view');
}

/** Whether the shell links to a section: anyone may search; the rest need their permission. */
function navVisible(auth: Auth, permission: string | null): boolean {
  return permission === null || auth.can(permission);
}

const SIDEBAR_KEY = 'console.sidebar.collapsed';

/**
 * Sidebar width, remembered across sessions.
 *
 * Collapsed the rail keeps only its icons, which hands ~150px back to the page — worth having on
 * the Data tab, where horizontal room is the scarce resource.
 */
function useSidebar(): [boolean, () => void] {
  const [collapsed, setCollapsed] = useState(() => {
    try {
      return localStorage.getItem(SIDEBAR_KEY) === '1';
    } catch {
      return false;
    }
  });
  const toggle = useCallback(() => {
    setCollapsed((current) => {
      const next = !current;
      try {
        localStorage.setItem(SIDEBAR_KEY, next ? '1' : '0');
      } catch {
        /* private browsing; the preference simply does not persist */
      }
      return next;
    });
  }, []);
  return [collapsed, toggle];
}

function useTheme(): [boolean, () => void] {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains('dark'));
  const toggle = useCallback(() => {
    setDark((current) => {
      const next = !current;
      document.documentElement.classList.toggle('dark', next);
      localStorage.setItem('console.theme', next ? 'dark' : 'light');
      return next;
    });
  }, []);
  return [dark, toggle];
}

/** One component and the build it is running; `null` reads as unavailable rather than absent. */
function VersionRow({
  label,
  hint,
  version,
}: {
  label: string;
  hint: string;
  version: string | null;
}): ReactNode {
  return (
    <div className="flex items-baseline justify-between gap-4 py-2.5">
      <div className="min-w-0">
        <dt className="text-[13px] font-medium">{label}</dt>
        <dd className="mt-0.5 text-[12px] text-[var(--color-ink-muted)]">{hint}</dd>
      </div>
      <span className="numeric shrink-0 text-[13px]">
        {version ? (
          `v${version}`
        ) : (
          <span className="text-[var(--color-ink-faint)]" title="Could not be reached">
            unavailable
          </span>
        )}
      </span>
    </div>
  );
}

export function LiveIndicator({
  status,
  generatedAt,
  error,
}: {
  status: LiveStatus;
  generatedAt?: string | null;
  error?: string | null;
}): ReactNode {
  const now = useTicker(1000);
  const healthy = status === 'live' && !error;

  return (
    <span
      className="inline-flex items-center gap-2 text-[12px] text-[var(--color-ink-muted)]"
      title={error ?? (generatedAt ? `Last update ${generatedAt}` : undefined)}
    >
      <span
        className={cn(
          'size-1.5 rounded-full',
          healthy ? 'live-dot bg-[var(--color-phase-running)]' : 'bg-[var(--color-phase-paused)]',
        )}
      />
      {healthy
        ? generatedAt
          ? `Live · ${formatRelativeTime(generatedAt, now)}`
          : 'Live'
        : status === 'connecting'
          ? 'Connecting…'
          : (error ?? 'Reconnecting…')}
    </span>
  );
}

export function AppShell({ children }: { children: ReactNode }): ReactNode {
  const [dark, toggleTheme] = useTheme();
  const [collapsed, toggleSidebar] = useSidebar();
  const [aboutOpen, setAboutOpen] = useState(false);
  const consoleAuth = useConsoleAuth();
  const scope = useScopeDomainId();
  // The shell sits around a domain's pages, so it asks the address which domain they are of.
  const auth = scope ? consoleAuth.forDomain(scope) : consoleAuth.overall;
  const navItems = NAV_ITEMS.filter((item) => navVisible(auth, item.permission)).map((item) => ({
    ...item,
    to: sectionPath(item.section, scope),
    end: item.section === 'overview',
  }));
  const settingsVisible = canOpenSettings(auth) || canOpenSettings(consoleAuth.overall);
  const settingsTo = sectionPath('settings', scope);
  const consoleConfig = useConsoleConfig();
  const domains = consoleConfig.data?.domains ?? [];
  const troubled = domains.filter((domain) => domain.status === 'misconfigured' || domain.status === 'incompatible');

  return (
    <div className="flex h-full min-h-0">
      <aside
        className={cn(
          'hidden shrink-0 flex-col border-r border-[var(--color-line)] bg-[var(--color-surface)] md:flex',
          'transition-[width] duration-150',
          collapsed ? 'w-[60px]' : 'w-[216px]',
        )}
      >
        <div className={cn('flex h-14 items-center gap-2.5', collapsed ? 'justify-center px-2' : 'px-5')}>
          <span className="grid size-6 shrink-0 place-items-center rounded-[7px] bg-[var(--color-ink)]">
            <span className="size-2 rounded-[2px] bg-[var(--color-canvas)]" />
          </span>
          {collapsed ? null : (
            <span className="truncate text-[13px] font-semibold tracking-tight">Midstream</span>
          )}
        </div>

        {domains.length > 1 ? <DomainSwitcher domains={domains} scope={scope} collapsed={collapsed} /> : null}

        <nav className={cn('flex flex-1 flex-col gap-0.5 py-2', collapsed ? 'px-2' : 'px-3')}>
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              title={collapsed ? item.label : undefined}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-2.5 rounded-[var(--radius-control)] py-2 text-[13px] font-medium transition-colors',
                  collapsed ? 'justify-center px-0' : 'px-2.5',
                  isActive
                    ? 'bg-[var(--color-raised)] text-[var(--color-ink)]'
                    : 'text-[var(--color-ink-muted)] hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]',
                )
              }
            >
              <item.icon className="size-4 shrink-0" />
              {collapsed ? <span className="sr-only">{item.label}</span> : item.label}
            </NavLink>
          ))}
          {settingsVisible ? (
            <NavLink
              to={settingsTo}
              title={collapsed ? SETTINGS_ITEM.label : undefined}
              className={({ isActive }) =>
                cn(
                  'mt-auto flex items-center gap-2.5 rounded-[var(--radius-control)] py-2 text-[13px] font-medium transition-colors',
                  collapsed ? 'justify-center px-0' : 'px-2.5',
                  isActive
                    ? 'bg-[var(--color-raised)] text-[var(--color-ink)]'
                    : 'text-[var(--color-ink-muted)] hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]',
                )
              }
            >
              <SETTINGS_ITEM.icon className="size-4 shrink-0" />
              {collapsed ? <span className="sr-only">{SETTINGS_ITEM.label}</span> : SETTINGS_ITEM.label}
            </NavLink>
          ) : null}
        </nav>

        <div
          className={cn(
            'flex items-center border-t border-[var(--color-line)] py-3',
            collapsed ? 'flex-col gap-1 px-2' : 'justify-between gap-1 px-3',
          )}
        >
          <AccountMenu collapsed={collapsed} />
          <Button variant="ghost" size="sm" onClick={toggleTheme} aria-label="Toggle theme" className="px-2">
            {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={toggleSidebar}
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            className="px-2"
          >
            {collapsed ? <PanelLeftOpen className="size-4" /> : <PanelLeftClose className="size-4" />}
          </Button>
        </div>

        <button
          type="button"
          onClick={() => setAboutOpen(true)}
          title="About this deployment"
          className={cn(
            'numeric flex items-center gap-1.5 border-t border-[var(--color-line)] py-2 text-[11px]',
            'text-[var(--color-ink-faint)] transition-colors hover:text-[var(--color-ink)]',
            collapsed ? 'justify-center px-2' : 'px-4',
          )}
        >
          <Info className="size-3.5 shrink-0" />
          {collapsed ? (
            <span className="sr-only">About this deployment</span>
          ) : (
            <span className="truncate">v{consoleConfig.data?.version ?? '—'}</span>
          )}
        </button>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <nav className="flex items-center gap-1 border-b border-[var(--color-line)] px-4 py-2 md:hidden">
          {[...navItems, ...(settingsVisible ? [{ ...SETTINGS_ITEM, to: settingsTo, end: false }] : [])].map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  'rounded-[var(--radius-control)] px-3 py-1.5 text-[13px] font-medium',
                  isActive ? 'bg-[var(--color-raised)]' : 'text-[var(--color-ink-muted)]',
                )
              }
            >
              {item.label}
            </NavLink>
          ))}
          {consoleAuth.overall.signedIn ? null : (
            <NavLink
              to="/sign-in"
              className="ml-auto rounded-[var(--radius-control)] px-3 py-1.5 text-[13px] font-medium text-[var(--color-ink)]"
            >
              Sign in
            </NavLink>
          )}
        </nav>
        {troubled.map((domain) => (
          <p
            key={domain.id}
            role="alert"
            className="border-b border-[color-mix(in_srgb,var(--color-phase-failed)_35%,transparent)] bg-[color-mix(in_srgb,var(--color-phase-failed)_8%,transparent)] px-5 py-2 text-[12px] text-[var(--color-phase-failed)]"
          >
            {domain.statusMessage}
          </p>
        ))}
        <main className="min-h-0 flex-1 overflow-y-auto">{children}</main>
      </div>

      <Dialog
        open={aboutOpen}
        onClose={() => setAboutOpen(false)}
        title="About this deployment"
        description="Each component is deployed independently, so their versions can differ."
        footer={
          <Button variant="secondary" onClick={() => setAboutOpen(false)}>
            Close
          </Button>
        }
      >
        <dl className="divide-y divide-[var(--color-line)]">
          <VersionRow
            label="Console"
            hint="This web application"
            version={consoleConfig.data?.version ?? null}
          />
          {domains.map((domain) => (
            <VersionRow
              key={domain.id}
              label={domains.length > 1 ? `${domain.name} control plane` : 'Control plane'}
              hint={`Serves domain ${domain.id}: its API, and reconciles its materializations`}
              version={domain.version}
            />
          ))}
        </dl>
        {consoleConfig.data?.federationKeysAgree === false ? (
          <p className="mt-4 text-[12px] leading-relaxed text-[var(--color-phase-failed)]">
            The domains sign access tokens with different federation keys: a token made in one is refused in another.
            Give every domain the same federation-key.
          </p>
        ) : null}
        <p className="mt-4 text-[12px] leading-relaxed text-[var(--color-ink-muted)]">
          A materialization&rsquo;s runtime reports its own version, shown on that
          materialization&rsquo;s Overview. A runtime keeps its build until its spec is re-applied,
          so it can lag the control plane.
        </p>
      </Dialog>

    </div>
  );
}

/**
 * Which part of the console the pages are about: every domain, or one (handbook 20). Choosing keeps
 * the section - the overview, the lists, settings - that is open.
 */
function DomainSwitcher({ domains, scope, collapsed }: { domains: ConsoleDomain[]; scope: string | null; collapsed: boolean }): ReactNode {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const section = sectionOf(pathname);
  const current = domains.find((domain) => domain.id === scope) ?? null;
  const choose = (domain: string | null): void => {
    void navigate(sectionPath(section === 'search' ? 'overview' : section, domain));
  };
  const dot = (domain: ConsoleDomain): ReactNode => (
    <span
      className={cn(
        'size-1.5 shrink-0 rounded-full',
        domain.status === 'ok' && domain.connected ? 'bg-[var(--color-phase-running)]' : domain.status === 'misconfigured' || domain.status === 'incompatible' ? 'bg-[var(--color-phase-failed)]' : 'bg-[var(--color-phase-paused)]',
      )}
      title={domain.statusMessage ?? (domain.connected ? 'Live' : 'Reconnecting')}
    />
  );
  const itemClass =
    'flex w-full items-center gap-2.5 rounded-[var(--radius-control)] px-2.5 py-2 text-left text-[13px] transition-colors hover:bg-[var(--color-raised)]';
  return (
    <div className={cn('pb-1', collapsed ? 'px-2' : 'px-3')}>
      <Popover
        side="bottom"
        width={232}
        trigger={({ open, toggle, id }) => (
          <button
            type="button"
            onClick={toggle}
            aria-expanded={open}
            aria-controls={id}
            aria-label="Choose a domain"
            title={collapsed ? (current?.name ?? 'All domains') : undefined}
            className={cn(
              'flex w-full items-center gap-2 rounded-[var(--radius-control)] border border-[var(--color-line)] py-1.5 text-[13px] font-medium transition-colors hover:bg-[var(--color-raised)]',
              collapsed ? 'justify-center px-0' : 'px-2.5',
            )}
          >
            {current ? dot(current) : <Layers className="size-4 shrink-0 text-[var(--color-ink-muted)]" />}
            {collapsed ? null : <span className="min-w-0 flex-1 truncate text-left">{current?.name ?? 'All domains'}</span>}
            {collapsed ? null : <ChevronsUpDown className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />}
          </button>
        )}
      >
        {({ close }) => (
          <div className="p-1.5" role="menu">
            <button
              type="button"
              role="menuitem"
              className={itemClass}
              onClick={() => {
                close();
                choose(null);
              }}
            >
              <Layers className="size-4 shrink-0 text-[var(--color-ink-muted)]" />
              <span className="flex-1">All domains</span>
              {scope === null ? <Check className="size-3.5 text-[var(--color-accent)]" /> : null}
            </button>
            <div className="my-1 border-t border-[var(--color-line)]" />
            {domains.map((domain) => (
              <button
                key={domain.id}
                type="button"
                role="menuitem"
                disabled={domain.status === 'misconfigured'}
                className={cn(itemClass, domain.status === 'misconfigured' && 'opacity-50')}
                onClick={() => {
                  close();
                  choose(domain.id);
                }}
              >
                {dot(domain)}
                <span className="min-w-0 flex-1 truncate">{domain.name}</span>
                {scope === domain.id ? <Check className="size-3.5 text-[var(--color-accent)]" /> : null}
              </button>
            ))}
          </div>
        )}
      </Popover>
    </div>
  );
}

/**
 * The person using the page, with their account and signing out of every domain; or, for someone
 * signed in nowhere, the way to sign in.
 */
function AccountMenu({ collapsed }: { collapsed: boolean }): ReactNode {
  const consoleAuth = useConsoleAuth();
  const auth = consoleAuth.overall;
  const domains = useConsoleConfig().data?.domains ?? [];
  const signedInNames = consoleAuth.signedInDomains.map((id) => domains.find((domain) => domain.id === id)?.name ?? id);
  const navigate = useNavigate();
  const location = useLocation();
  const itemClass =
    'flex w-full items-center gap-2.5 rounded-[var(--radius-control)] px-2.5 py-2 text-left text-[13px] text-[var(--color-ink-muted)] transition-colors hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]';

  if (!auth.signedIn) {
    return (
      <button
        type="button"
        onClick={() => navigate('/sign-in', { state: { from: location } })}
        title={collapsed ? 'Sign in' : undefined}
        className={cn(
          'flex min-w-0 items-center gap-2 rounded-[var(--radius-control)] py-1.5 text-[12px] font-medium text-[var(--color-ink)] transition-colors hover:bg-[var(--color-raised)]',
          collapsed ? 'justify-center px-2' : 'px-2',
        )}
      >
        <LogIn className="size-3.5 shrink-0" />
        {collapsed ? <span className="sr-only">Sign in</span> : <span className="truncate">Sign in</span>}
      </button>
    );
  }

  const principal = auth.principal;
  return (
    <Popover
      side="top"
      width={248}
      trigger={({ open, toggle, id }) => (
        <button
          type="button"
          onClick={toggle}
          aria-expanded={open}
          aria-controls={id}
          aria-label="Your account"
          title={collapsed ? principal.display_name : undefined}
          className={cn(
            'flex min-w-0 items-center gap-2 rounded-[var(--radius-control)] py-1 text-[12px] text-[var(--color-ink-muted)] transition-colors hover:bg-[var(--color-raised)] hover:text-[var(--color-ink)]',
            collapsed ? 'justify-center px-1' : 'max-w-[124px] px-1.5',
          )}
        >
          <Avatar name={principal.display_name} email={principal.email} size="sm" />
          {collapsed ? null : <span className="truncate">{principal.first_name ?? principal.display_name}</span>}
        </button>
      )}
    >
      {({ close }) => (
        <div className="p-1.5">
          <div className="flex items-center gap-2.5 px-2.5 pt-1.5 pb-2.5">
            <Avatar name={principal.display_name} email={principal.email} />
            <div className="min-w-0">
              <p className="truncate text-[13px] font-medium">{principal.display_name}</p>
              <p className="truncate text-[12px] text-[var(--color-ink-muted)]">{principal.email}</p>
            </div>
          </div>
          {domains.length > 1 ? (
            <p className="px-2.5 pb-2.5 text-[12px] text-[var(--color-ink-muted)]">
              Signed in to {signedInNames.join(', ')}
              {signedInNames.length < domains.length ? (
                <>
                  {' · '}
                  <button type="button" className="font-medium text-[var(--color-accent)] hover:underline" onClick={() => { close(); navigate('/sign-in', { state: { from: location } }); }}>
                    sign in to more
                  </button>
                </>
              ) : null}
            </p>
          ) : null}
          <div className="border-t border-[var(--color-line)] pt-1.5">
            <button
              type="button"
              className={itemClass}
              onClick={() => {
                close();
                navigate('/settings/account');
              }}
            >
              <UserRound className="size-4" />
              Your account
            </button>
            <button
              type="button"
              className={itemClass}
              onClick={() => {
                close();
                void auth.signOut().then(() => navigate('/', { replace: true }));
              }}
            >
              <LogOut className="size-4" />
              {domains.length > 1 ? 'Sign out everywhere' : 'Sign out'}
            </button>
          </div>
        </div>
      )}
    </Popover>
  );
}

export function PageHeader({
  title,
  subtitle,
  actions,
  meta,
  children,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  meta?: ReactNode;
  children?: ReactNode;
}): ReactNode {
  return (
    <header className="border-b border-[var(--color-line)] bg-[var(--color-surface)]">
      <div className="mx-auto flex max-w-[1400px] flex-wrap items-start justify-between gap-4 px-6 pt-6 pb-4">
        <div className="min-w-0">
          <h1 className="truncate text-[20px] leading-tight font-semibold tracking-tight">{title}</h1>
          {subtitle ? (
            <div className="mt-1 text-[13px] text-[var(--color-ink-muted)]">{subtitle}</div>
          ) : null}
          {meta ? <div className="mt-2.5 flex flex-wrap items-center gap-2">{meta}</div> : null}
        </div>
        {actions ? <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div> : null}
      </div>
      {children}
    </header>
  );
}

export function PageBody({ children, className }: { children: ReactNode; className?: string }): ReactNode {
  return <div className={cn('mx-auto max-w-[1400px] px-6 py-6', className)}>{children}</div>;
}
