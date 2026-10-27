/** The console's addresses (handbook 20): its own pages, and each domain's under `/d/<domain>/`. */

/** A link inside a domain: `/d/<domain>/<path>`. */
export function domainPath(domain: string, path = ''): string {
  return `/d/${encodeURIComponent(domain)}${path.startsWith('/') || path === '' ? path : `/${path}`}`;
}

/** The part of the console an address is in: an overview, the lists, search or settings. */
export type ShellSection = 'overview' | 'materializations' | 'datasets' | 'search' | 'settings';

/** Which part of the console an address is in, whatever its scope. */
export function sectionOf(pathname: string): ShellSection {
  const rest = pathname.replace(/^\/d\/[^/]+/, '') || '/';
  if (rest.startsWith('/materializations')) return 'materializations';
  if (rest.startsWith('/datasets')) return 'datasets';
  if (rest.startsWith('/search')) return 'search';
  if (rest.startsWith('/settings')) return 'settings';
  return 'overview';
}

/** Where a section is: in a domain, or across every domain. Search is always the console's. */
export function sectionPath(section: ShellSection, domain: string | null): string {
  const path = section === 'overview' ? '' : `/${section}`;
  if (domain === null || section === 'search') return path || '/';
  return domainPath(domain, path);
}
