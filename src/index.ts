// YASD - Yet Another Simple Database
// A SQL-like in-memory database for Node.js, plus an O(1) KV cache fast
// path (for Echo hot feeds, channel lists, presence/typing, rate limits).

import { Executor, SlowQueryEntry, QueryPlan, QueryProfile } from './executor';
import { KVCache, KVOptions, KVStats, KVBatchEntry, KVTransaction, TransactionError, TxResult, SnapshotEntry, DEFAULT_NAMESPACE_TTLS } from './cache';
import { PubSubHub, PubSubListener, INVALIDATE_CHANNEL, InvalidationEvent } from './pubsub';
import { parse } from './parser';
import {
  QueryResult,
  Row,
  Value,
  Primitive,
  JsonValue,
  TableSchema,
  SqlStatement
} from './types';

// Re-export types
export type {
  QueryResult,
  Row,
  Value,
  Primitive,
  JsonValue,
  TableSchema,
  SqlStatement,
  KVOptions,
  KVStats,
  KVBatchEntry,
  TxResult,
  SnapshotEntry,
  PubSubListener,
  InvalidationEvent
};

export { parse, KVCache, KVTransaction, TransactionError, DEFAULT_NAMESPACE_TTLS, PubSubHub, INVALIDATE_CHANNEL };
export { SlowLog, SLOW_LOG_CAP, checkSlowThreshold } from './metrics';
export type { SlowEntry } from './metrics';
export { YasdServer, serverOptionsFromEnv, DEFAULT_HOST, DEFAULT_PORT } from './server'
export type { YasdServerOptions, YasdServerTlsOptions, ServerInfo } from './server';
export { YasdClient, parseCacheUrl } from './client';
export type { YasdClientOptions, ParsedCacheUrl, SubscribeHandler, TxExecResult, YasdTransactionOptions } from './client';
export { YasdTransaction } from './client';
export { saveSnapshot, loadSnapshot, AofLog, applyAofOp } from './persistence';
export type {
  SnapshotFile,
  SnapshotStore,
  SnapshotSaveOptions,
  SnapshotLoadMetadata,
  SnapshotLoadOptions,
  AofBatchEntry,
  AofOp,
  AofMutation,
} from './persistence';
export {
  RespDecoder,
  encodeCommand,
  encodeReply,
  encodeSimple,
  encodeError,
  encodeInt,
  encodeBulk,
  encodeArray,
  requestArgv,
} from './protocol';
export type { RespReply, RespDecoderOptions } from './protocol';

/**
 * YASD constructor options: KV cache tuning plus SQL observability.
 * The legacy single-arg form `new YASD(kvOptions)` still works.
 */
export interface YasdOptions extends KVOptions {
  /** Log SQL queries slower than this (ms) into the slow-query log. 0 = off. */
  slowQueryMs?: number;
}

export type {
  SlowQueryEntry,
  QueryPlan,
  QueryProfile,
};

/**
 * YASD Database class
 * Provides a SQL-like interface for in-memory database operations, plus a
 * fast KV cache (`get/set/del/clearPrefix`) that never touches the SQL
 * parser on the hot path.
 */
export class YASD {
  private executor: Executor;
  private cache: KVCache;
  private hub: PubSubHub;

  constructor(cacheOptions?: KVOptions) {
    this.executor = new Executor();
    const opts = cacheOptions as YasdOptions | undefined;
    this.cache = new KVCache(cacheOptions);
    if (opts?.slowQueryMs !== undefined) {
      this.executor.setSlowQueryThreshold(opts.slowQueryMs);
    }
    this.hub = new PubSubHub();
  }

  /**
   * Execute a SQL query
   * @param sql - The SQL query to execute
   * @returns QueryResult containing the results
   * @example
   * ```typescript
   * const db = new YASD();
   * db.query('CREATE TABLE users (id int, name string)');
   * db.query('INSERT INTO users VALUES (1, "John")');
   * const result = db.query('SELECT * FROM users');
   * console.log(result.rows); // [{ id: 1, name: 'John' }]
   * ```
   */
  query(sql: string): QueryResult {
    return this.executor.query(sql);
  }

  /**
   * Explain how a SELECT is served without running it: index-scan vs
   * full-scan for the WHERE filter, plus ORDER BY / LIMIT / OFFSET shape
   * and the live row count. Non-SELECT statements report strategy 'n/a'.
   * @example
   * ```typescript
   * db.explain("SELECT * FROM users WHERE id = 7 ORDER BY age DESC LIMIT 10");
   * // { statement: 'select', strategy: 'index-scan', indexColumns: ['id'],
   * //   hasOrderBy: true, orderBy: { column: 'age', direction: 'desc' },
   * //   limit: 10, tableRows: 200, ... }
   * ```
   */
  explain(sql: string): QueryPlan {
    return this.executor.explain(sql);
  }

  /** Run a query and report timing + shape (plan, rows, duration). */
  profile(sql: string): QueryProfile {
    return this.executor.profile(sql);
  }

  /** Log SQL slower than this (ms); 0 disables. */
  setSlowQueryThreshold(ms: number): void {
    this.executor.setSlowQueryThreshold(ms);
  }

  /** Newest-first slow-query ring (capped at 100). */
  slowLog(): SlowQueryEntry[] {
    return this.executor.getSlowLog();
  }

  clearSlowLog(): void {
    this.executor.clearSlowLog();
  }

  // ---- KV fast path (O(1), no SQL parsing) ----

  /**
   * Cached lookup. Lazy-expires entries and touches LRU recency.
   * @returns the value, or undefined on miss/expiry.
   */
  get(key: string): Value | undefined {
    return this.cache.get(key);
  }

  /**
   * Cache a value (objects/arrays supported).
   * @param ttlMs explicit TTL in ms; when omitted the per-namespace default
   * (e.g. feeds ~15s, popular 60s, channel lists 30s) or `defaultTTLMs`
   * applies; otherwise the key persists. Must be finite and >= 0.
   * @returns the stored value (mirrors `memory.js` cacheSet).
   */
  set(key: string, value: Value, ttlMs?: number): Value {
    return this.cache.set(key, value, ttlMs);
  }

  /** Delete a key. Returns false when missing/expired. */
  del(key: string): boolean {
    return this.cache.del(key);
  }

  /**
   * Delete a whole namespace (`cacheClear(namespace)` seam): removes
   * `prefix` and every `prefix:*` key. Returns the number removed.
   */
  clearPrefix(prefix: string): number {
    return this.cache.clearPrefix(prefix);
  }

  /** Ms remaining; -1 = persists; -2 = missing/expired. */
  ttl(key: string): number {
    return this.cache.ttl(key);
  }

  /** Replace a key's TTL. Returns false when missing/expired. */
  expire(key: string, ttlMs: number): boolean {
    return this.cache.expire(key, ttlMs);
  }

  /** Drop a key's TTL so it persists. Returns false when missing/expired. */
  persist(key: string): boolean {
    return this.cache.persist(key);
  }

  /** Cache hit/miss/eviction/expiry counters plus size. */
  cacheStats(): KVStats {
    return this.cache.stats();
  }

  /** Zero the cache counters (hits/misses/evictions/expiries); data kept. */
  resetStats(): void {
    this.cache.resetStats();
  }

  /** Synchronously evict expired keys. Returns the number removed. */
  cacheSweep(): number {
    return this.cache.sweep();
  }

  // ---- atomic counters (rate limits, unread/like counts) ----

  /**
   * Atomic increment (missing key counts from 0, TTL preserved).
   * Throws on non-numeric values. Returns the new value.
   */
  incr(key: string, by = 1): number {
    return this.cache.incr(key, by);
  }

  /** Atomic decrement. Returns the new value. */
  decr(key: string, by = 1): number {
    return this.cache.decr(key, by);
  }

  /**
   * Compare-and-set for read-modify-write. Succeeds when the live value
   * deep-equals `expected` (`undefined` asserts absence). Returns true on
   * success, false leaving state untouched. Explicit `ttlMs` wins, else the
   * existing TTL is preserved.
   */
  cas(key: string, expected: Value | undefined, value: Value, ttlMs?: number): boolean {
    return this.cache.cas(key, expected, value, ttlMs);
  }

  // ---- batch ops (feed hydration) ----

  /** Batch read; values in key order (`undefined` on miss). */
  mget(keys: string[]): Array<Value | undefined> {
    return this.cache.mget(keys);
  }

  /** Batch write; returns the number of entries written. */
  mset(entries: KVBatchEntry[]): number {
    return this.cache.mset(entries);
  }

  // ---- transactions (multi-key read-modify-write) ----

  /**
   * Start an optimistic transaction. Watch keys, read, queue writes, commit:
   * ```typescript
   * const tx = db.multi();
   * tx.watch('likes:1');
   * const likes = (tx.get('likes:1') as number) ?? 0;
   * tx.set('likes:1', likes + 1);
   * const results = tx.exec(); // null = conflict, retry
   * ```
   */
  multi(): KVTransaction {
    return this.cache.multi();
  }

  /**
   * Watch-then-commit loop with retries (see `KVCache.runTransaction`).
   * Retries the whole read-modify-write on conflict up to `maxRetries` times.
   */
  runTransaction<T>(
    keys: string[],
    fn: (tx: KVTransaction) => T | Promise<T>,
    maxRetries = 3
  ): Promise<{ committed: boolean; attempts: number; results: TxResult[] | null; value: T | undefined }> {
    return this.cache.runTransaction(keys, fn, maxRetries);
  }

  /** Mutation generation for `key` (0 = never written) — WATCH plumbing. */
  keyVersion(key: string): number {
    return this.cache.getVersion(key);
  }

  // ---- persistence helpers (snapshot/restore the embedded cache) ----

  /** Live entries for snapshots (expired keys skipped). */
  dump(): SnapshotEntry[] {
    return this.cache.dump();
  }

  /** Restore snapshot entries (absolute expiry preserved). */
  restore(entries: SnapshotEntry[]): number {
    return this.cache.restore(entries);
  }

  /** Clear cached entries (tables untouched; use reset() for everything). */
  clear(): void {
    this.cache.clear();
  }

  // ---- pub/sub (invalidation, presence/typing) ----

  /** Publish a string message; returns the subscriber count. */
  publish(channel: string, message: string): number {
    return this.hub.publish(channel, message);
  }

  /** Subscribe; returns an unsubscribe function. */
  subscribe(channel: string, listener: PubSubListener): () => void {
    return this.hub.subscribe(channel, listener);
  }

  /** Subscriber count (optionally for one channel). */
  subscriberCount(channel?: string): number {
    return this.hub.subscriberCount(channel);
  }

  // ---- tables ----

  /**
   * Get list of all table names
   */
  getTableNames(): string[] {
    return this.executor.getTableNames();
  }

  /**
   * Get schema for a specific table
   * @param tableName - Name of the table
   * @returns TableSchema or undefined if table doesn't exist
   */
  getTableSchema(tableName: string): TableSchema | undefined {
    return this.executor.getTableSchema(tableName);
  }

  /**
   * Reset the database (clear all tables, data, and cached entries)
   */
  reset(): void {
    this.executor.reset();
    this.cache.clear();
  }

  /**
   * Graceful shutdown: stops the TTL sweeper so the process can exit.
   */
  close(): void {
    this.cache.close();
  }

  /**
   * Parse SQL into AST without executing
   * @param sql - SQL to parse
   * @returns Parsed SQL statement AST
   */
  static parse(sql: string): SqlStatement {
    return parse(sql);
  }

  /**
   * Create a new YASD instance with pre-loaded schema
   * @param schema - SQL schema definition
   * @returns New YASD instance
   */
  static fromSchema(schema: string): YASD {
    const db = new YASD();
    db.query(schema);
    return db;
  }
}

/**
 * Create a new YASD database instance
 */
export function createDatabase(cacheOptions?: YasdOptions): YASD {
  return new YASD(cacheOptions);
}

// Export default
const db = new YASD();
export default db;
