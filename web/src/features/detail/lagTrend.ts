// Type-only, so `node --test` can load this module without the bundler's path aliases.
import type { LagSample } from '@shared/types';

export type LagState = 'caught-up' | 'draining' | 'growing' | 'steady' | 'unknown';

export interface LagTrend {
  state: LagState;
  /** Change in lag, records per second: negative while the runtime gains on the topic. */
  perSecond: number | null;
  /** Time left to reach zero at the current pace, while draining. */
  etaMs: number | null;
}

/**
 * Which way consumer lag is heading, and how fast.
 *
 * The rate is the least-squares slope through the measurements of the last `windowMs`, so one
 * measurement taken just before or after a commit does not swing it; with fewer than three there,
 * the last few measurements stand in. A change smaller than 0.1% of the lag over the window — or a
 * single record — is noise, and reads as steady.
 */
export function lagTrend(history: LagSample[], windowMs = 10 * 60_000): LagTrend {
  const latest = history.at(-1);
  if (!latest) return { state: 'unknown', perSecond: null, etaMs: null };
  if (latest.lag === 0) return { state: 'caught-up', perSecond: 0, etaMs: null };

  let points = history.filter((point) => point.t >= latest.t - windowMs);
  if (points.length < 3) points = history.slice(-5);
  if (points.length < 2) return { state: 'unknown', perSecond: null, etaMs: null };

  const meanT = points.reduce((sum, point) => sum + point.t, 0) / points.length;
  const meanLag = points.reduce((sum, point) => sum + point.lag, 0) / points.length;
  let covariance = 0;
  let variance = 0;
  for (const point of points) {
    covariance += (point.t - meanT) * (point.lag - meanLag);
    variance += (point.t - meanT) ** 2;
  }
  if (variance === 0) return { state: 'unknown', perSecond: null, etaMs: null };
  const perSecond = (covariance / variance) * 1000;

  const spanSeconds = ((points.at(-1) as LagSample).t - (points[0] as LagSample).t) / 1000;
  const change = Math.abs(perSecond) * spanSeconds;
  if (change < Math.max(1, latest.lag * 0.001)) return { state: 'steady', perSecond, etaMs: null };
  if (perSecond > 0) return { state: 'growing', perSecond, etaMs: null };
  return { state: 'draining', perSecond, etaMs: (latest.lag / -perSecond) * 1000 };
}

/** `3h 10m`, `12m`, `45s`: a duration to read at a glance, to the nearest sensible unit. */
export function formatEta(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '—';
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ${minutes % 60}m`;
  return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}
