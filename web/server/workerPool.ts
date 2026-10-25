/**
 * Size Node's worker pool for DuckDB, before anything starts it.
 *
 * A query issued through `@duckdb/node-api` holds one libuv worker thread from start to finish,
 * including every moment it spends waiting on object storage. The pool defaults to four threads,
 * so four queries were all that could ever be in flight: a run told to count 24 objects at once
 * counted four, and file reads, DNS lookups and manifest downloads queued behind them. Measured
 * from a laptop, 200 cold object counts took 12.8 s on the default pool and 4.9 s on 64 threads.
 *
 * The pool is sized for every run scanning at full width at once, plus room for page reads,
 * exports and everything else that uses it. libuv reads `UV_THREADPOOL_SIZE` once, when the pool
 * first starts, so `main.ts` imports this module before anything else. An explicit setting wins.
 */

import { config } from './config.ts';

/** Threads beyond the scans: page reads, exports, profiles, the catalog, files and DNS. */
const RESERVE = 32;

/** libuv's ceiling. */
const MAXIMUM = 1024;

export const workerPoolSize = Math.min(MAXIMUM, config.runConcurrency * config.runScanConcurrency + RESERVE);

process.env.UV_THREADPOOL_SIZE ??= String(workerPoolSize);
