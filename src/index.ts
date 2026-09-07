// YASD - Yet Another Simple Database
// A SQL-like in-memory database for Node.js, plus an O(1) KV cache fast
// path (for Echo hot feeds, channel lists, presence/typing, rate limits).

import { Executor } from './executor';
import { KVCache, KVOptions, KVStats, DEFAULT_NAMESPACE_TTLS } from './cache';
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
  KVStats
};

export { parse, KVCache, DEFAULT_NAMESPACE_TTLS };

/**
 * YASD Database class
 * Provides a SQL-like interface for in-memory database operations, plus a
 * fast KV cache (`get/set/del/clearPrefix`) that never touches the SQL
 * parser on the hot path.
 */
export class YASD {
  private executor: Executor;
  private cache: KVCache;

  constructor(cacheOptions?: KVOptions) {
    this.executor = new Executor();
    this.cache = new KVCache(cacheOptions);
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

  /** Synchronously evict expired keys. Returns the number removed. */
  cacheSweep(): number {
    return this.cache.sweep();
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
export function createDatabase(cacheOptions?: KVOptions): YASD {
  return new YASD(cacheOptions);
}

// Export default
const db = new YASD();
export default db;
