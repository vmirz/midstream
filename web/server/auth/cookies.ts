/**
 * The session cookie: how a signed-in browser carries its session to the console.
 *
 * It holds the control plane's session token and is HttpOnly, so no script on the page can read
 * it, and SameSite=Lax, so another site cannot make the browser send it with a change. The console
 * keeps no session state of its own; the cookie and the control plane are the whole of it.
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

/** A `Set-Cookie` value for the session cookie. A max age of 0 deletes it. */
export function sessionCookie(value: string, options: CookieOptions): string {
  const parts = [
    `${config.sessionCookieName}=${encodeURIComponent(value)}`,
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
