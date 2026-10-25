/**
 * Waiting for the directory agent to apply a change: to users, groups or roles, to datasets and
 * their access, or a claim of new tables' names.
 *
 * The control plane accepts a change at once and its directory agent applies it a moment later, so
 * a screen that showed the accepted change straight away could show it before it happened. Like a
 * settings save, the console waits — up to 10 seconds — for the outcome, and answers with it.
 */

import { controlPlane, type Caller } from '../controlPlane/client.ts';
import { ConsoleError } from '../errors.ts';
import type { CpIdentityAccepted, CpIdentityOutcome, IdentityChangeResult } from '../../shared/types.ts';
import { forgetAllSessions } from './principal.ts';

const APPLY_TIMEOUT_MS = 10_000;
const APPLY_POLL_MS = 150;

/** Wait for an accepted change; returns it applied or pending, and throws when it was refused. */
export async function settle(caller: Caller, accepted: CpIdentityAccepted): Promise<IdentityChangeResult> {
  const outcome = await waitForOutcome(caller, accepted.request_id);
  if (outcome.status === 'rejected') throw refusal(outcome);
  if (outcome.status === 'applied') {
    // Whatever changed may change what anyone may do; every session is checked again.
    forgetAllSessions();
  }
  return {
    request_id: accepted.request_id,
    status: outcome.status === 'applied' ? 'applied' : 'pending',
    entity_type: accepted.entity_type,
    entity_id: accepted.entity_id,
  };
}

export async function waitForOutcome(caller: Caller, requestId: string): Promise<CpIdentityOutcome> {
  const deadline = Date.now() + APPLY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    try {
      const outcome = await controlPlane.identityRequest(caller, requestId);
      if (outcome.status !== 'pending') return outcome;
    } catch (cause) {
      // Ending the asker's own sessions — a password change — makes their session unusable the
      // moment it applies; the caller decides what that means.
      if (cause instanceof ConsoleError && cause.statusCode === 401) {
        return { request_id: requestId, status: 'rejected', reason: 'session_ended', message: cause.message };
      }
      // Any other failed read is retried until the deadline; the change itself was accepted.
    }
    await new Promise((resolve) => setTimeout(resolve, APPLY_POLL_MS));
  }
  return { request_id: requestId, status: 'pending' };
}

/**
 * The error for a change the identity agent refused, with what it said about the refusal: for a
 * claim of table names, which table and dataset (`details`).
 */
export function refusal(outcome: CpIdentityOutcome): ConsoleError {
  const message = outcome.message ?? 'The change was refused.';
  const details: Record<string, unknown> = { ...(outcome.details ?? {}), reason: outcome.reason ?? 'rejected' };
  if (outcome.field) details.field = outcome.field;
  switch (outcome.reason) {
    case 'invalid':
      return new ConsoleError('invalid_request', message, 400, details);
    case 'not_found':
    case 'dataset_not_found':
      return new ConsoleError('not_found', message, 404, details);
    case 'forbidden':
      return new ConsoleError('forbidden', message, 403, details);
    case 'already_exists':
      return new ConsoleError('already_exists', message, 409, details);
    default:
      return new ConsoleError('conflict', message, 409, details);
  }
}
