import { useMutation } from '@tanstack/react-query';
import { useCallback, useState } from 'react';

import { useToast } from '@/components/ui/Toast';
import { ApiError, domainApi } from '@/lib/api';
import { useOptionalDomain } from '@/lib/domains';
import type { CpDetail } from '@shared/types';

export type LifecycleAction = 'stop' | 'resume' | 'delete';

const LABELS: Record<LifecycleAction, { verb: string; done: string }> = {
  stop: { verb: 'Stopping', done: 'Stop requested' },
  resume: { verb: 'Resuming', done: 'Resume requested' },
  delete: { verb: 'Deleting', done: 'Delete requested' },
};

/**
 * Lifecycle mutations against the control plane.
 *
 * Every mutation is accepted asynchronously (202), so the UI never optimistically rewrites
 * state — it relies on the live stream to reflect what actually happened.
 */
export function useLifecycleActions(): {
  /** Act on a materialization of `domain`: the page's own, inside a domain. */
  run: (action: LifecycleAction, materializationId: string, domain?: string) => void;
  pending: LifecycleAction | null;
} {
  const scope = useOptionalDomain();
  const toast = useToast();
  const [pending, setPending] = useState<LifecycleAction | null>(null);

  const mutation = useMutation({
    mutationFn: async ({ action, id, domain }: { action: LifecycleAction; id: string; domain: string }) => {
      const api = domainApi(domain);
      if (action === 'stop') return api.stop(id);
      if (action === 'resume') return api.resume(id);
      return api.remove(id);
    },
    onSuccess: (accepted, variables) => {
      toast.success(
        LABELS[variables.action].done,
        `${accepted.materialization_id} → ${accepted.desired_phase}. Watch the phase for convergence.`,
      );
    },
    onError: (error: unknown, variables) => {
      toast.error(
        `${LABELS[variables.action].verb} failed`,
        error instanceof ApiError ? error.message : 'The control plane rejected the request.',
      );
    },
    onSettled: () => setPending(null),
  });

  const run = useCallback(
    (action: LifecycleAction, materializationId: string, domain: string | undefined = scope?.id) => {
      if (!domain) throw new Error('say which domain the materialization is in');
      setPending(action);
      mutation.mutate({ action, id: materializationId, domain });
    },
    [mutation, scope?.id],
  );

  return { run, pending };
}

/**
 * Whether the runtime exists to act on. Stopping and rescaling both re-apply it, which needs every
 * one of its tables; the control plane refuses either before they are all created.
 */
export function hasRuntime(detail: CpDetail): boolean {
  const created = new Set(detail.tables.map((table) => table.name));
  return detail.materialization.tables.length > 0 && detail.materialization.tables.every((table) => created.has(table.name));
}

function isLeaving(detail: CpDetail): boolean {
  const desired = detail.materialization.desired_phase ?? detail.status.desired_phase;
  return desired === 'deleting' || desired === 'deleted';
}

/**
 * Offered while the materialization is meant to run. Decided by the desired phase, not the observed
 * one: a materialization still converging toward running can be stopped, and one already stopping
 * offers Resume instead.
 */
export function canStop(detail: CpDetail): boolean {
  const desired = detail.status.desired_phase;
  return (desired === 'running' || desired === 'paused') && hasRuntime(detail) && !isLeaving(detail);
}

/** Offered while the materialization is meant to be stopped, including while it is stopping. */
export function canResume(detail: CpDetail): boolean {
  const desired = detail.status.desired_phase;
  return (desired === 'stopped' || desired === 'paused') && !isLeaving(detail);
}

export function canEdit(detail: CpDetail): boolean {
  return hasRuntime(detail) && !isLeaving(detail);
}

export function canDelete(detail: CpDetail): boolean {
  const phase = detail.status.observed_phase;
  return phase !== 'deleting' && phase !== 'deleted';
}
