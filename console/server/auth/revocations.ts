/**
 * Access token revocations, handed between domains (handbook 20).
 *
 * A token is checked by every domain on its own, so revoking one must reach every domain. The domain
 * that made it records the revocation and signs it with the federation key; the console hands that
 * to every other domain at once, and a domain that could not be reached then is told again: every
 * five minutes the console reads each domain's revocations since it last looked and hands them on.
 * A domain records a revocation only when the signature checks out, and records each once.
 */

import { SERVICE } from '../controlPlane/client.ts';
import type { Domain } from '../domains/domain.ts';
import { logger } from '../logger.ts';
import type { CpTokenRevocation } from '../../shared/types.ts';

/** How often every domain's revocations are read and handed on. */
export const REVOCATION_SYNC_MS = 5 * 60 * 1000;
/** The most revocations one request hands over, as a domain takes them. */
const BATCH = 100;

/** Hand revocations from `source` to every other domain; says how each answered. */
export async function handOn(
  revocations: readonly CpTokenRevocation[],
  source: Domain,
  all: readonly Domain[],
): Promise<Record<string, 'applied' | 'failed'>> {
  const results: Record<string, 'applied' | 'failed'> = {};
  await Promise.all(
    all
      .filter((domain) => domain.id !== source.id)
      .map(async (domain) => {
        // A revocation that came from this domain in the first place is not handed back to it.
        const due = revocations.filter((revocation) => revocation.source !== domain.id);
        try {
          for (let start = 0; start < due.length; start += BATCH) {
            await domain.controlPlane.applyTokenRevocations(SERVICE, due.slice(start, start + BATCH), source.id);
          }
          results[domain.id] = 'applied';
        } catch (cause) {
          logger.warn({ err: cause, from: source.id, to: domain.id }, 'token revocations could not be handed to a domain; it is told again within five minutes');
          results[domain.id] = 'failed';
        }
      }),
  );
  return results;
}

/** The five-minute pass: each domain's revocations since the last one, handed to the others. */
export class RevocationSync {
  private readonly cursors = new Map<string, string>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private running: Promise<number> | null = null;

  private readonly served: () => readonly Domain[];

  constructor(served: () => readonly Domain[]) {
    this.served = served;
  }

  start(): void {
    this.timer = setInterval(() => void this.syncOnce(), REVOCATION_SYNC_MS);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** One pass; returns how many revocations were handed on. Concurrent callers share one pass. */
  syncOnce(): Promise<number> {
    this.running ??= this.pass().finally(() => {
      this.running = null;
    });
    return this.running;
  }

  private async pass(): Promise<number> {
    const all = this.served();
    if (all.length < 2) return 0;
    let handed = 0;
    for (const domain of all) {
      let revocations: CpTokenRevocation[];
      try {
        ({ revocations } = await domain.controlPlane.tokenRevocations(SERVICE, this.cursors.get(domain.id) ?? null));
      } catch (cause) {
        logger.debug({ err: cause, domain: domain.id }, 'token revocations could not be read from a domain this pass');
        continue;
      }
      if (revocations.length === 0) continue;
      const results = await handOn(revocations, domain, all);
      handed += revocations.length;
      // Only once every other domain has them does the next pass start after them.
      if (Object.values(results).every((result) => result === 'applied')) {
        const newest = revocations.reduce((latest, revocation) => ((revocation.revoked_at ?? '') > latest ? (revocation.revoked_at ?? '') : latest), '');
        if (newest) this.cursors.set(domain.id, newest);
      }
    }
    return handed;
  }
}
