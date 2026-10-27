import { createHash } from 'node:crypto';
import { promisify } from 'node:util';
import { gzip as gzipCallback } from 'node:zlib';

import { config } from '../config.ts';
import { SERVICE, type ControlPlaneClient } from '../controlPlane/client.ts';
import { ConsoleError, unavailable } from '../errors.ts';
import { logger, type Logger } from '../logger.ts';
import type { CpTopicList, TopicCatalogResponse } from '../../shared/types.ts';

const gzip = promisify(gzipCallback);

/** A catalog response prepared once per refresh and handed to every reader as-is. */
export interface CatalogBody {
  json: Buffer;
  gzip: Buffer;
  /** Weak, because the same content is served in two encodings. */
  etag: string;
  topicCount: number;
}

export interface TopicCatalogOptions {
  ttlMs: number;
  minRefreshMs: number;
  now?: () => number;
  /** Where a failed refresh is logged: the domain's log, naming it. */
  log?: Logger;
}

/**
 * The cluster's topic list, shared by every browser this process serves.
 *
 * Every open of the create dialog reads it, so it is built for many readers at once:
 *
 * - **One upstream request per refresh.** Concurrent reads share the request in flight.
 * - **Stale while revalidating.** A list older than the TTL is still answered immediately while a
 *   background refresh replaces it; only the first read after start waits for the control plane.
 * - **Prepared once.** The JSON body, its gzip encoding, and its ETag are built per refresh, so a
 *   read costs one buffer write, or a bodiless 304 when the browser already holds this version.
 * - **Bounded.** No refresh starts within `minRefreshMs` of the previous attempt, so explicit
 *   refreshes, retries after a failure, and stale reads cannot multiply load on the control plane.
 *
 * Nothing runs while nobody reads: there is no timer, only refreshes caused by reads.
 */
export class TopicCatalog {
  private body: CatalogBody | null = null;
  private response: TopicCatalogResponse | null = null;
  private loadedAt = Number.NEGATIVE_INFINITY;
  private lastAttemptAt = Number.NEGATIVE_INFINITY;
  private lastError: string | null = null;
  private inFlight: Promise<void> | null = null;
  private readonly fetchTopics: (refresh: boolean) => Promise<CpTopicList>;
  private readonly options: TopicCatalogOptions;
  private readonly now: () => number;

  constructor(fetchTopics: (refresh: boolean) => Promise<CpTopicList>, options: TopicCatalogOptions) {
    this.fetchTopics = fetchTopics;
    this.options = options;
    this.now = options.now ?? Date.now;
  }

  /**
   * The current catalog. `refresh` asks for a reload first, which is honoured only when the last
   * attempt is older than `minRefreshMs`; otherwise the reader gets the copy already held.
   */
  async read({ refresh = false }: { refresh?: boolean } = {}): Promise<CatalogBody> {
    const now = this.now();
    const mayRefresh = now - this.lastAttemptAt >= this.options.minRefreshMs;
    if (this.inFlight && !this.body) {
      await this.inFlight;
    } else if (refresh && mayRefresh) {
      await this.refresh(true);
    } else if (!this.body && mayRefresh) {
      await this.refresh(false);
    } else if (this.body && mayRefresh && now - this.loadedAt >= this.options.ttlMs) {
      void this.refresh(false);
    }
    if (!this.body) {
      throw unavailable('The Kafka topic list is unavailable right now', {
        reason: this.lastError ?? 'not loaded yet',
      });
    }
    return this.body;
  }

  /** Start loading without waiting, so the first dialog open does not pay for it. */
  warm(): void {
    if (!this.body && !this.inFlight) void this.refresh(false);
  }

  /** Resolves once no refresh is in flight, as after a stale read has started one in the background. */
  async settled(): Promise<void> {
    while (this.inFlight) await this.inFlight;
  }

  /** Never rejects: background refreshes are not awaited, and Node exits on an unhandled rejection. */
  private refresh(force: boolean): Promise<void> {
    this.inFlight ??= (async () => {
      this.lastAttemptAt = this.now();
      try {
        const response = toCatalogResponse(await this.fetchTopics(force));
        this.body = await prepare(response);
        this.response = response;
        this.loadedAt = this.now();
        this.lastError = null;
      } catch (cause) {
        this.lastError = describeFailure(cause);
        (this.options.log ?? logger).warn({ err: cause }, 'topic catalog refresh failed');
        // Keep serving the last good list, marked so the picker can say it may be out of date.
        if (this.response) {
          try {
            this.body = await prepare({ ...this.response, stale: true, error: this.lastError });
          } catch {
            /* the unmarked copy keeps being served */
          }
        }
      } finally {
        this.inFlight = null;
      }
    })();
    return this.inFlight;
  }
}

export function toCatalogResponse(list: CpTopicList): TopicCatalogResponse {
  if (!list || !Array.isArray(list.items)) {
    throw new Error('the control plane returned a topic list without items');
  }
  return {
    topics: list.items
      .filter((item) => typeof item?.topic === 'string' && item.topic.length > 0)
      .map((item) => ({
        name: item.topic,
        partitions: Number(item.partitions) || 0,
        replicationFactor: Number(item.replication_factor) || 0,
        ...(item.category ? { category: item.category } : {}),
      })),
    fetchedAt: list.fetched_at,
    stale: list.stale === true,
    error: list.error ?? null,
  };
}

async function prepare(response: TopicCatalogResponse): Promise<CatalogBody> {
  const json = Buffer.from(JSON.stringify(response));
  return {
    json,
    gzip: await gzip(json, { level: 6 }),
    etag: `W/"${createHash('sha1').update(json).digest('base64url')}"`,
    topicCount: response.topics.length,
  };
}

function describeFailure(cause: unknown): string {
  if (cause instanceof ConsoleError && cause.statusCode === 404) {
    // A console deployed ahead of its control plane: say what to do rather than echo a 404.
    return 'the control plane does not list topics yet; upgrade it to enable the topic picker';
  }
  if (cause instanceof ConsoleError) {
    const upstream = cause.details.upstream_details as { reason?: unknown } | undefined;
    const reason = upstream?.reason ?? cause.details.reason;
    return typeof reason === 'string' && reason ? `${cause.message}: ${reason}` : cause.message;
  }
  return cause instanceof Error ? cause.message : String(cause);
}

/** RFC 9110 weak comparison of an `If-None-Match` header against the current ETag. */
export function etagMatches(header: string | undefined, etag: string): boolean {
  if (!header) return false;
  const opaque = (tag: string): string => tag.trim().replace(/^W\//, '');
  const current = opaque(etag);
  return header.split(',').some((candidate) => candidate.trim() === '*' || opaque(candidate) === current);
}

/** Whether `Accept-Encoding` admits gzip, honouring an explicit `q=0` refusal. */
export function acceptsGzip(header: string | undefined): boolean {
  if (!header) return false;
  return header.split(',').some((part) => {
    const [coding, ...params] = part.trim().toLowerCase().split(';');
    if (coding?.trim() !== 'gzip') return false;
    const quality = params.map((param) => param.trim()).find((param) => param.startsWith('q='));
    return quality === undefined || Number(quality.slice(2)) > 0;
  });
}

/** One domain's topic catalog, read with its service token. */
export function topicCatalogFor(controlPlane: ControlPlaneClient, log: Logger = logger): TopicCatalog {
  // Getters, so a change made on the Settings page applies to the next read.
  return new TopicCatalog((refresh) => controlPlane.topics(SERVICE, refresh), {
    log,
    get ttlMs() {
      return config.topicCatalogTtlMs;
    },
    get minRefreshMs() {
      return config.topicCatalogMinRefreshMs;
    },
  });
}
