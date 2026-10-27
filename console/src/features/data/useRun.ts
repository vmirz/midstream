import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useRef, useState } from 'react';

import { pageQueryKey } from './live.ts';
import { ApiError } from '@/lib/api';
import { useApi } from '@/lib/domains';
import type { RunPage, RunRefresh, RunRequest, RunSnapshot, TimeRange } from '@shared/types';

export { pageQueryKey, pageStage } from './live.ts';

export interface RunController {
  /** The run on screen, kept current from its event stream. */
  run: RunSnapshot | null;
  /** A run is being created. */
  starting: boolean;
  /** Why the last run could not be created — a bad statement, a filter on a missing column. */
  startError: ApiError | null;
  start: (request: RunRequest) => Promise<RunSnapshot | null>;
  cancel: () => void;
  /** Pick up a run that already exists, e.g. after a page refresh. False when it is gone. */
  attach: (runId: string) => Promise<boolean>;
  /**
   * Bring the run on screen up to date: the live tail's newest records appended to it, or a new run
   * in its place, followed from then on. `range` is its range resolved again, for a new run.
   * Rejects with the API's error; a run that has expired answers 404.
   */
  refresh: (range: TimeRange | null) => Promise<RunRefresh | null>;
}

/**
 * One run at a time, followed live.
 *
 * Starting a run cancels the one before it if it is still working: a new question supersedes the
 * old one, and leaving it to finish would spend the server on an answer nobody will look at.
 * Progress arrives over server-sent events; if the stream drops, the hook polls instead, because a
 * status frozen by a dead transport is indistinguishable from a hung query.
 */
export function useRun(materializationId: string): RunController {
  const api = useApi();
  const [run, setRun] = useState<RunSnapshot | null>(null);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<ApiError | null>(null);
  const current = useRef<RunSnapshot | null>(null);
  const source = useRef<EventSource | null>(null);
  const poll = useRef<number | null>(null);

  const show = useCallback((snapshot: RunSnapshot) => {
    if (current.current && current.current.id !== snapshot.id) return;
    current.current = snapshot;
    setRun(snapshot);
  }, []);

  const stopFollowing = useCallback(() => {
    source.current?.close();
    source.current = null;
    if (poll.current !== null) {
      window.clearInterval(poll.current);
      poll.current = null;
    }
  }, []);

  const follow = useCallback(
    (snapshot: RunSnapshot) => {
      stopFollowing();
      current.current = snapshot;
      setRun(snapshot);
      if (snapshot.state !== 'running') return;

      const id = snapshot.id;
      const events = new EventSource(api.runStreamUrl(id));
      source.current = events;
      events.onmessage = (event) => {
        try {
          const next = JSON.parse(event.data as string) as RunSnapshot;
          show(next);
          if (next.state !== 'running') stopFollowing();
        } catch {
          /* a malformed frame is superseded by the next one */
        }
      };
      events.onerror = () => {
        if (source.current !== events) return;
        events.close();
        source.current = null;
        poll.current ??= window.setInterval(() => {
          void api
            .run(id)
            .then((next) => {
              show(next);
              if (next.state !== 'running') stopFollowing();
            })
            .catch(() => undefined);
        }, 1_000);
      };
    },
    [show, stopFollowing],
  );

  const start = useCallback(
    async (request: RunRequest): Promise<RunSnapshot | null> => {
      const previous = current.current;
      if (previous && previous.state === 'running') void api.cancelRun(previous.id).catch(() => undefined);
      stopFollowing();
      current.current = null;
      setStarting(true);
      setStartError(null);
      try {
        const snapshot = await api.startRun(materializationId, request);
        follow(snapshot);
        return snapshot;
      } catch (error) {
        setStartError(error instanceof ApiError ? error : new ApiError('request_failed', String(error), 0));
        return null;
      } finally {
        setStarting(false);
      }
    },
    [materializationId, follow, stopFollowing],
  );

  const cancel = useCallback(() => {
    const active = current.current;
    if (!active || active.state !== 'running') return;
    void api
      .cancelRun(active.id)
      .then((snapshot) => {
        show(snapshot);
        stopFollowing();
      })
      .catch(() => undefined);
  }, [show, stopFollowing]);

  const attach = useCallback(
    async (runId: string): Promise<boolean> => {
      try {
        const snapshot = await api.run(runId);
        if (snapshot.materializationId !== materializationId) return false;
        follow(snapshot);
        return true;
      } catch {
        return false;
      }
    },
    [materializationId, follow],
  );

  const refresh = useCallback(
    async (range: TimeRange | null): Promise<RunRefresh | null> => {
      const active = current.current;
      if (!active) return null;
      const answer = await api.refreshRun(active.id, range);
      // The run on screen may have changed while the refresh was out; it decides, not this answer.
      if (current.current?.id !== active.id) return answer;
      if (answer.replaced) follow(answer.run);
      else show(answer.run);
      return answer;
    },
    [follow, show],
  );

  useEffect(() => stopFollowing, [stopFollowing]);

  return { run, starting, startError, start, cancel, attach, refresh };
}

/**
 * One page of the run on screen.
 *
 * A page is requested as soon as any of its rows are ready and again as more arrive, so the grid
 * fills while a run is still counting. A complete page is immutable — the run is pinned to the
 * data as it was when it started — so it is cached for good, and the next page is fetched ahead of
 * the click that will ask for it. That is what makes paging feel instant. Only a refresh that
 * appends the live tail's newest records changes a finished run's rows, and it bumps the run's
 * `revision`, which is part of every page's key.
 */
export function useRunPage(run: RunSnapshot | null, offset: number, limit: number) {
  const api = useApi();
  const queryClient = useQueryClient();
  const terminal = run !== null && run.state !== 'running';
  const ready = run ? Math.min(run.rowsReady, offset + limit) : 0;
  const runId = run?.id ?? null;

  const query = useQuery<RunPage>({
    queryKey: pageQueryKey(run, offset, limit),
    queryFn: () => api.runRows(runId as string, offset, limit),
    enabled: runId !== null && (ready > offset || terminal),
    staleTime: Number.POSITIVE_INFINITY,
    gcTime: 5 * 60_000,
    retry: 0,
    // Within one run, keep showing the rows on screen while the next read lands; never carry one
    // run's rows into another's.
    placeholderData: (previous, previousQuery) =>
      previousQuery?.queryKey[1] === runId ? keepPreviousData(previous) : undefined,
  });

  const nextOffset = offset + limit;
  const nextReady = run !== null && (run.rowsReady >= nextOffset + limit || (terminal && run.rowsReady > nextOffset));
  useEffect(() => {
    if (!runId || !nextReady || !query.data?.complete) return;
    void queryClient.prefetchQuery({
      queryKey: pageQueryKey(run, nextOffset, limit),
      queryFn: () => api.runRows(runId, nextOffset, limit),
      staleTime: Number.POSITIVE_INFINITY,
    });
  }, [queryClient, runId, nextOffset, limit, nextReady, query.data?.complete, run]);

  // The last page is the other place people jump to, so it is read as soon as the total is final.
  const pageable = run === null ? 0 : run.request.kind === 'sql' ? (run.rowsRetained ?? 0) : run.rows;
  const lastOffset = Math.max(0, Math.ceil(pageable / limit) - 1) * limit;
  const settled = run !== null && run.state === 'succeeded' && run.totalExact;
  useEffect(() => {
    if (!runId || !settled || lastOffset <= nextOffset) return;
    void queryClient.prefetchQuery({
      queryKey: pageQueryKey(run, lastOffset, limit),
      queryFn: () => api.runRows(runId, lastOffset, limit),
      staleTime: Number.POSITIVE_INFINITY,
    });
    // Once per run, revision and page size: the run object changes with every snapshot.
  }, [queryClient, runId, run?.revision, settled, lastOffset, limit]);

  return query;
}
