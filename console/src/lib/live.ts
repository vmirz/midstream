import { useEffect, useRef, useState } from 'react';

import { notifySessionLost } from './api';
import { useAuth } from './auth';
import type { DetailSnapshot, FleetSnapshot } from '@shared/types';

/** Least time between two session checks a failing stream asks for. */
const SESSION_CHECK_INTERVAL_MS = 30_000;

export type LiveStatus = 'connecting' | 'live' | 'reconnecting';

interface LiveResult<T> {
  data: T | null;
  status: LiveStatus;
}

/**
 * Subscribe to a server-sent-events channel with automatic reconnect.
 *
 * `EventSource` already retries, but it surfaces no state; tracking it here lets the UI show an
 * honest freshness indicator instead of silently rendering stale data.
 */
function useEventStream<T>(url: string, eventName: string): LiveResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [status, setStatus] = useState<LiveStatus>('connecting');
  const everConnected = useRef(false);
  const lastSessionCheck = useRef(0);
  // A new person reconnects the stream, so it carries their session rather than the last one's.
  const { generation } = useAuth();

  useEffect(() => {
    setStatus(everConnected.current ? 'reconnecting' : 'connecting');
    const source = new EventSource(url);

    const onMessage = (event: MessageEvent<string>): void => {
      everConnected.current = true;
      setStatus('live');
      try {
        setData(JSON.parse(event.data) as T);
      } catch {
        /* ignore malformed frames; the next tick supersedes them */
      }
    };
    // The server ends a stream whose caller may no longer see it, and says so first.
    const onAccessLost = (): void => {
      source.close();
      setStatus('reconnecting');
      notifySessionLost();
    };

    source.addEventListener(eventName, onMessage as EventListener);
    source.addEventListener('auth', onAccessLost);
    source.onopen = () => setStatus('live');
    source.onerror = () => {
      setStatus(everConnected.current ? 'reconnecting' : 'connecting');
      // A stream also fails when its session has gone; check, but not on every retry.
      const now = Date.now();
      if (now - lastSessionCheck.current > SESSION_CHECK_INTERVAL_MS) {
        lastSessionCheck.current = now;
        notifySessionLost();
      }
    };

    return () => {
      source.removeEventListener(eventName, onMessage as EventListener);
      source.removeEventListener('auth', onAccessLost);
      source.close();
    };
  }, [url, eventName, generation]);

  return { data, status };
}

export function useFleetStream(): LiveResult<FleetSnapshot> {
  return useEventStream<FleetSnapshot>('/api/stream/fleet', 'fleet');
}

export function useDetailStream(materializationId: string): LiveResult<DetailSnapshot> {
  return useEventStream<DetailSnapshot>(
    `/api/stream/materializations/${encodeURIComponent(materializationId)}`,
    'detail',
  );
}

/** Re-render on an interval so relative timestamps stay honest without a data fetch. */
export function useTicker(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}
