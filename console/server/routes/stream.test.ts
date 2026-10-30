/**
 * The merged fleet stream (handbook 20): its first frame at once, as a single domain's was before
 * domains, and changes from several domains close together merged into one frame.
 */

import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { after, before, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import { installTestAuth, testPrincipal } from '../auth/testing.ts';
import { installTestDomains } from '../domains/testing.ts';
import type { FleetSnapshot } from '../../shared/types.ts';
import { MERGE_DELAY_MS, registerFleetStreamRoutes } from './stream.ts';

const domains = await installTestDomains(['domain_a', 'domain_b']);
const listeners = new Map<string, (snapshot: FleetSnapshot) => void>();

function snapshot(): FleetSnapshot {
  return { generatedAt: new Date().toISOString(), connected: true, controlPlaneError: null, totals: {} as FleetSnapshot['totals'], items: [], series: [] };
}

for (const domain of domains.all()) {
  domain.live.fleetSnapshot = snapshot;
  domain.live.subscribeFleet = (listener) => {
    listeners.set(domain.id, listener);
    listener(snapshot());
    return () => listeners.delete(domain.id);
  };
}

let server: FastifyInstance;
let base: string;

before(async () => {
  server = Fastify();
  installTestAuth(server, testPrincipal(['materializations.view']));
  await registerFleetStreamRoutes(server);
  await server.listen({ port: 0, host: '127.0.0.1' });
  base = `http://127.0.0.1:${(server.server.address() as AddressInfo).port}`;
});

after(async () => {
  await server.close();
});

/**
 * Read frames from a stream until `count` have arrived or `ms` pass; each with when it arrived, after
 * the response itself - not counting the connection and the sign-in check, which a busy machine slows.
 */
async function frames(count: number, ms: number, during?: () => void): Promise<{ frames: { at: number; data: string }[]; started: number }> {
  const controller = new AbortController();
  const response = await fetch(`${base}/api/stream/fleet`, { signal: controller.signal });
  const started = performance.now();
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const seen: { at: number; data: string }[] = [];
  let buffer = '';
  const deadline = setTimeout(() => controller.abort(), ms);
  let triggered = false;
  try {
    while (seen.length < count) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value);
      let end: number;
      while ((end = buffer.indexOf('\n\n')) !== -1) {
        const frame = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        if (frame.startsWith('event: fleet')) seen.push({ at: performance.now() - started, data: frame });
      }
      if (seen.length >= 1 && during && !triggered) {
        triggered = true;
        during();
      }
    }
  } catch {
    // The deadline: whatever arrived is the answer.
  } finally {
    clearTimeout(deadline);
    controller.abort();
  }
  return { frames: seen, started };
}

describe('the merged fleet stream', () => {
  it('sends its first frame at once, with every domain in it', async () => {
    const { frames: seen } = await frames(1, 2_000);
    assert.equal(seen.length, 1);
    // Not held back by the merge delay, as the frames after it are.
    assert.ok((seen[0]?.at ?? Infinity) < MERGE_DELAY_MS, `the first frame took ${seen[0]?.at} ms after the response`);
    const body = JSON.parse(seen[0]!.data.split('\ndata: ')[1] ?? '{}') as { domains: { domain: string }[] };
    assert.deepEqual(body.domains.map((entry) => entry.domain), ['domain_a', 'domain_b']);
  });

  it('merges changes from several domains close together into one frame', async () => {
    const { frames: seen } = await frames(3, 600, () => {
      listeners.get('domain_a')?.(snapshot());
      listeners.get('domain_b')?.(snapshot());
    });
    assert.equal(seen.length, 2, 'the first frame, then one for both changes');
  });
});
