/**
 * Sign-in attempts per client address, counted over a sliding minute.
 *
 * This is the console's own first line: it turns a flood away before it reaches the control
 * plane, where each attempt costs a deliberately slow password check. The control plane still
 * locks an email out after repeated wrong passwords, whichever address they come from. Counts
 * live in this process, so each console replica counts its own.
 */

const WINDOW_MS = 60_000;
const MAX_TRACKED = 50_000;

export class AttemptLimiter {
  private readonly attempts = new Map<string, number[]>();
  private readonly windowMs: number;

  constructor(windowMs = WINDOW_MS) {
    this.windowMs = windowMs;
  }

  /** Record an attempt for `key`; false when it is over `limit` in the window, and not counted. */
  allow(key: string, limit: number, now = Date.now()): boolean {
    const recent = (this.attempts.get(key) ?? []).filter((at) => now - at < this.windowMs);
    if (recent.length >= limit) {
      this.attempts.set(key, recent);
      return false;
    }
    recent.push(now);
    this.attempts.delete(key);
    this.attempts.set(key, recent);
    this.prune(now);
    return true;
  }

  /** Seconds until `key` may try again. */
  retryAfter(key: string, now = Date.now()): number {
    const oldest = (this.attempts.get(key) ?? [])[0];
    return oldest === undefined ? 0 : Math.max(1, Math.ceil((oldest + this.windowMs - now) / 1000));
  }

  private prune(now: number): void {
    if (this.attempts.size <= MAX_TRACKED) return;
    for (const [key, times] of this.attempts) {
      if (this.attempts.size <= MAX_TRACKED) break;
      if (times.every((at) => now - at >= this.windowMs) || this.attempts.size > MAX_TRACKED) this.attempts.delete(key);
    }
  }
}
