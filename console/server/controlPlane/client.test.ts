/**
 * The control-plane client tells a control plane's own answer from a gateway's: a gateway whose
 * control plane is not there answers 404 or 5xx with a page of its own, which is the domain being
 * unreachable - not, say, a materialization it says was deleted.
 */

import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { after, describe, it } from 'node:test';

import { ConsoleError } from '../errors.ts';
import { ControlPlaneClient, SERVICE, isControlPlaneError } from './client.ts';

const answers: Record<string, { status: number; type?: string; body: string }> = {
  '/materializations/mat-gone': { status: 404, type: 'application/json', body: '{"error":{"code":"not_found","message":"materialization mat-gone was not found","details":{}}}' },
  '/materializations/mat-gateway': { status: 404, body: '' },
  '/materializations/mat-html': { status: 502, type: 'text/html', body: '<html><body>Bad gateway</body></html>' },
  '/settings/control-plane': { status: 503, type: 'application/json', body: '{"error":{"code":"unavailable","message":"the settings table is starting","details":{}}}' },
};
const server = createServer((request, response) => {
  const answer = answers[new URL(request.url ?? '/', 'http://stub').pathname] ?? { status: 404, type: 'text/plain', body: '404: Not Found' };
  response.writeHead(answer.status, answer.type ? { 'content-type': answer.type } : {}).end(answer.body);
});
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
const client = new ControlPlaneClient(`http://127.0.0.1:${(server.address() as AddressInfo).port}`, 'token');

after(() => {
  server.closeAllConnections();
  server.close();
});

async function failure(promise: Promise<unknown>): Promise<ConsoleError> {
  try {
    await promise;
  } catch (cause) {
    assert.ok(cause instanceof ConsoleError);
    return cause;
  }
  assert.fail('expected a refusal');
}

describe('a control plane’s answers and a gateway’s', () => {
  it('passes the control plane’s own errors on with their status', async () => {
    const gone = await failure(client.detail(SERVICE, 'mat-gone'));
    assert.equal(gone.statusCode, 404);
    assert.match(gone.message, /mat-gone was not found/);
    const starting = await failure(client.settingsScope(SERVICE, 'control-plane'));
    assert.equal(starting.statusCode, 503);
    assert.notEqual(starting.code, 'control_plane_unreachable');
  });

  it('calls a gateway’s bare 404 or HTML 502 the control plane being unreachable', async () => {
    for (const id of ['mat-gateway', 'mat-html']) {
      const refusal = await failure(client.detail(SERVICE, id));
      assert.equal(refusal.code, 'control_plane_unreachable', id);
      assert.equal(refusal.statusCode, 503, id);
      assert.match(refusal.message, /is unreachable: it answered (404|502) without the control plane's own error/);
    }
  });

  it('knows the control plane’s error by its JSON', () => {
    assert.equal(isControlPlaneError({ error: { code: 'not_found', message: 'x' } }), true);
    assert.equal(isControlPlaneError({ error: { code: 'invalid_upstream_payload', message: '<html>' } }), false);
    assert.equal(isControlPlaneError(null), false);
    assert.equal(isControlPlaneError({ items: [] }), false);
  });
});
