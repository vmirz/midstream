/**
 * The session cookies: how a signed-in browser carries its sessions to the console, one per domain.
 *
 * Each holds one domain's session token, `<CONSOLE_SESSION_COOKIE>_<domain id>`, and is HttpOnly, so
 * no script on the page can read it, and SameSite=Lax, so another site cannot make the browser send
 * it with a change. The console keeps no session state of its own; the cookies and the domains'
 * control planes are the whole of it.
 */

import { config } from '../config.ts';

/** Every cookie in a `Cookie` header, by name. Malformed pairs are skipped. */
export function parseCookies(header: string | undefined): Map<string, string> {
  const cookies = new Map<string, string>();
  if (!header) return cookies;
  for (const part of header.split(';')) {
    const separator = part.indexOf('=');
    if (separator <= 0) continue;
    const name = part.slice(0, separator).trim();
    const raw = part.slice(separator + 1).trim();
    if (!name || cookies.has(name)) continue;
    try {
      cookies.set(name, decodeURIComponent(raw.replace(/^"(.*)"$/, '$1')));
    } catch {
      // A value that is not valid percent-encoding is not one this console wrote.
    }
  }
  return cookies;
}

export interface CookieOptions {
  maxAgeSeconds: number;
  secure: boolean;
}

/** The name of a domain's session cookie. */
export function sessionCookieName(domainId: string): string {
  return `${config.sessionCookieName}_${domainId}`;
}

/** A `Set-Cookie` value for a domain's session cookie. A max age of 0 deletes it. */
export function sessionCookie(domainId: string, value: string, options: CookieOptions): string {
  return cookie(sessionCookieName(domainId), value, options);
}

/** A `Set-Cookie` value: HttpOnly, SameSite=Lax, for the whole site. A max age of 0 deletes it. */
export function cookie(name: string, value: string, options: CookieOptions): string {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    `Max-Age=${Math.max(0, Math.floor(options.maxAgeSeconds))}`,
  ];
  if (options.secure) parts.push('Secure');
  return parts.join('; ');
}

/** Whether the cookie should be Secure for a request that arrived over `protocol`. */
export function secureCookie(protocol: string): boolean {
  const setting = config.cookieSecure.trim().toLowerCase();
  if (setting === 'true') return true;
  if (setting === 'false') return false;
  return protocol === 'https';
}

/**
 * Add a cookie to a response without replacing the others it sets: a sign-in sets one per domain.
 * Fastify appends every `set-cookie` header it is given rather than replacing the last.
 */
export function appendCookie(reply: { header(name: string, value: unknown): unknown }, value: string): void {
  reply.header('set-cookie', value);
}
