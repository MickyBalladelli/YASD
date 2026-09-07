// KV fast path for YASD — O(1) cache for Echo hot paths (feeds, channel
// lists, popular posts, presence/typing, rate limits, unread counts).
//
// This deliberately bypasses SQL parsing: no `SELECT * FROM cache WHERE ...`
// on the per-request hot path. Use `get/set/del/clearPrefix` (Map-backed,
// LRU, size-aware) and keep SQL only for ad-hoc querying.
//
// TTL semantics (Redis-like):
//   - `set(key, value, ttlMs?)`: explicit ttlMs wins; otherwise the
//     per-namespace default applies; otherwise the key persists.
//     `ttlMs` must be a finite number >= 0 — `undefined`/`NaN` never
//     silently becomes "never expires" via NaN arithmetic (the `memory.js` bug).
//   - `ttl(key)`: ms remaining, `-1` = persists, `-2` = missing/expired.
//   - `expire(key, ttlMs)`: set/replace TTL, false if missing.
//   - `persist(key)`: drop TTL, false if missing.
// Expiry is lazy on `get`/`ttl`/`expire` plus an active background sweeper.

import { Value } from './types';

/** Per-namespace TTL defaults (ms). `feed`/`feeds` 15s, channel lists 30s, popular 60s. */
export const DEFAULT_NAMESPACE_TTLS: Record<string, number> = {
  feed: 15_000,
  feeds: 15_000,
  popular: 60_000,
  channel: 30_000,
  channels: 30_000,
  presence: 30_000,
  typing: 10_000,
  ratelimit: 60_000,
  'rate-limit': 60_000,
  unread: 30_000,
};

export interface KVOptions {
  /** Max live entries; oldest (LRU) evicted first. Default 10_000. */
  maxEntries?: number;
  /** Max total bytes (key + JSON value estimate). Default 64 MiB. */
  maxBytes?: number;
  /** Fallback TTL (ms) when no namespace default matches. Default: persist. */
  defaultTTLMs?: number;
  /** Per-namespace TTL defaults (ms). Merged over DEFAULT_NAMESPACE_TTLS. */
  namespaceTTLMs?: Record<string, number>;
  /** Background sweep interval (ms). <=0 disables. Default 1000. */
  sweepIntervalMs?: number;
}

export interface KVStats {
  hits: number;
  misses: number;
  evictions: number;
  expiries: number;
  entries: number;
  bytes: number;
}

/** One restorable cache entry (absolute expiry, for snapshots). */
export interface SnapshotEntry {
  key: string;
  value: Value;
  expiresAt?: number; // epoch ms; undefined = persists
}

/** One batched write for `mset` (feed hydration). */
export interface KVBatchEntry {
  key: string;
  value: Value;
  ttlMs?: number;
}

interface Entry {
  value: Value;
  expiresAt?: number; // epoch ms; undefined = persists
  size: number; // bytes estimate (key + value)
}

function estimateSize(key: string, value: Value): number {
  let jsonLen = 8;
  try {
    const json = JSON.stringify(value);
    if (json !== undefined) {
      if (typeof Buffer !== 'undefined' && typeof Buffer.byteLength === 'function') {
        jsonLen = Buffer.byteLength(json, 'utf8');
      } else {
        jsonLen = json.length;
      }
    }
  } catch {
    jsonLen = 64;
  }
  return key.length * 2 + jsonLen;
}

function validateTTL(ttlMs: number, what: string): number {
  if (typeof ttlMs !== 'number' || !Number.isFinite(ttlMs)) {
    throw new Error(`${what} must be a finite number of ms, got ${String(ttlMs)}`);
  }
  if (ttlMs < 0) {
    throw new Error(`${what} must be >= 0, got ${ttlMs}`);
  }
  return ttlMs;
}

export class KVCache {
  private map = new Map<string, Entry>();
  private bytes = 0;
  private hits = 0;
  private misses = 0;
  private evictions = 0;
  private expiries = 0;
  private timer?: ReturnType<typeof setInterval>;

  readonly maxEntries: number;
  readonly maxBytes: number;
  readonly defaultTTLMs?: number;
  readonly namespaceTTLMs: Record<string, number>;
  readonly sweepIntervalMs: number;

  constructor(options: KVOptions = {}) {
    this.maxEntries = options.maxEntries ?? 10_000;
    this.maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    this.defaultTTLMs = options.defaultTTLMs;
    this.namespaceTTLMs = { ...DEFAULT_NAMESPACE_TTLS, ...(options.namespaceTTLMs ?? {}) };
    this.sweepIntervalMs = options.sweepIntervalMs ?? 1000;
    if (this.sweepIntervalMs > 0) {
      this.startSweeper(this.sweepIntervalMs);
    }
  }

  /** Namespace = text before the first ':' (Echo `cacheKey(ns, v)` => `ns:v`). */
  private namespaceOf(key: string): string {
    const i = key.indexOf(':');
    return i > 0 ? key.slice(0, i) : '';
  }

  private resolveTTLMs(key: string, ttlMs?: number): number | undefined {
    if (ttlMs !== undefined) {
      return validateTTL(ttlMs, 'ttlMs');
    }
    const ns = this.namespaceOf(key);
    if (ns && this.namespaceTTLMs[ns] !== undefined) {
      return validateTTL(this.namespaceTTLMs[ns], `namespaceTTLMs[${ns}]`);
    }
    if (this.defaultTTLMs !== undefined) {
      return validateTTL(this.defaultTTLMs, 'defaultTTLMs');
    }
    return undefined; // persist
  }

  private isExpired(entry: Entry, now: number): boolean {
    return entry.expiresAt !== undefined && entry.expiresAt <= now;
  }

  private removeExpired(key: string, entry: Entry): void {
    this.map.delete(key);
    this.bytes -= entry.size;
    this.expiries++;
  }

  /** O(1) lookup with lazy expiry + LRU touch. */
  get(key: string): Value | undefined {
    const entry = this.map.get(key);
    if (!entry) {
      this.misses++;
      return undefined;
    }
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      this.misses++;
      return undefined;
    }
    // LRU touch: re-insert as most-recently-used.
    this.map.delete(key);
    this.map.set(key, entry);
    this.hits++;
    return entry.value;
  }

  /**
   * O(1) insert (amortized). Size-aware LRU eviction: after insert, evict
   * least-recently-used entries while over `maxEntries`/`maxBytes`.
   * A single oversized entry is kept (it evicts everything else).
   */
  set(key: string, value: Value, ttlMs?: number): Value {
    const ttl = this.resolveTTLMs(key, ttlMs);
    const expiresAt = ttl === undefined ? undefined : Date.now() + ttl;
    // Immediate expiry (ttl 0): behave like a write-through miss.
    if (expiresAt !== undefined && expiresAt <= Date.now()) {
      const old = this.map.get(key);
      if (old) {
        this.map.delete(key);
        this.bytes -= old.size;
      }
      this.expiries++;
      this.misses++;
      return value;
    }
    const size = estimateSize(key, value);
    const old = this.map.get(key);
    if (old) {
      this.bytes -= old.size;
      this.map.delete(key);
    }
    this.map.set(key, { value, expiresAt, size });
    this.bytes += size;
    this.evictIfNeeded(key);
    return value;
  }

  private evictIfNeeded(newestKey: string): void {
    while (
      (this.map.size > this.maxEntries && this.map.size > 1) ||
      (this.bytes > this.maxBytes && this.map.size > 1)
    ) {
      const oldest = this.map.keys().next();
      if (oldest.done) break;
      const oldestKey = oldest.value as string;
      if (oldestKey === newestKey) {
        // Newest is the only evictable left besides itself; if the map
        // holds >1 entry the oldest can't be newest here. Guard anyway:
        // rotate it to the back and stop to avoid evicting the just-write.
        break;
      }
      const entry = this.map.get(oldestKey);
      if (!entry) break;
      // Skip expired entries via accounting as expiries, not evictions.
      if (this.isExpired(entry, Date.now())) {
        this.removeExpired(oldestKey, entry);
      } else {
        this.map.delete(oldestKey);
        this.bytes -= entry.size;
        this.evictions++;
      }
    }
  }

  del(key: string): boolean {
    const entry = this.map.get(key);
    if (!entry) return false;
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      return false;
    }
    this.map.delete(key);
    this.bytes -= entry.size;
    return true;
  }

  /**
   * Delete all keys in a namespace. Matches `key === prefix` or
   * `key.startsWith(prefix + ':')` (Echo `cacheClear(namespace)` seam).
   * Returns the number of keys removed.
   */
  clearPrefix(prefix: string): number {
    const needle = prefix + ':';
    let count = 0;
    for (const key of Array.from(this.map.keys())) {
      if (key === prefix || key.startsWith(needle)) {
        const entry = this.map.get(key);
        if (entry) this.bytes -= entry.size;
        this.map.delete(key);
        count++;
      }
    }
    return count;
  }

  clear(): void {
    this.map.clear();
    this.bytes = 0;
  }

  /**
   * Batch read for feed hydration. Each element mirrors `get` (lazy expiry
   * + LRU touch per key). Returns values in key order (`undefined` on miss).
   */
  mget(keys: string[]): Array<Value | undefined> {
    return keys.map(k => this.get(k));
  }

  /**
   * Batch write for feed hydration. Applies `set` per entry (same TTL
   * resolution, validation, and size-aware LRU eviction).
   * Returns the number of entries written.
   */
  mset(entries: KVBatchEntry[]): number {
    for (const e of entries) {
      if (!e || typeof e.key !== 'string') {
        throw new Error('mset entries must be { key: string, value, ttlMs? }');
      }
      this.set(e.key, e.value, e.ttlMs);
    }
    return entries.length;
  }

  /**
   * Atomic counter add (single-threaded: read-modify-write is uninterrupted).
   * Missing key counts from 0; existing value must be a finite number.
   * TTL is preserved. Returns the new value.
   */
  incr(key: string, by = 1): number {
    if (typeof by !== 'number' || !Number.isFinite(by)) {
      throw new Error(`incr delta must be a finite number, got ${String(by)}`);
    }
    const now = Date.now();
    const entry = this.map.get(key);
    let base = 0;
    let expiresAt: number | undefined;
    if (entry) {
      if (this.isExpired(entry, now)) {
        this.removeExpired(key, entry);
      } else {
        if (typeof entry.value !== 'number' || !Number.isFinite(entry.value)) {
          throw new Error(`INCR requires a numeric value at '${key}'`);
        }
        base = entry.value;
        expiresAt = entry.expiresAt;
      }
    }
    const next = base + by;
    if (!Number.isFinite(next)) {
      throw new Error(`INCR overflow at '${key}'`);
    }
    const live = this.map.get(key);
    if (live && !this.isExpired(live, Date.now())) {
      this.bytes -= live.size;
      this.map.delete(key);
    }
    const size = estimateSize(key, next);
    this.map.set(key, { value: next, expiresAt, size });
    this.bytes += size;
    this.evictIfNeeded(key);
    return next;
  }

  /** Atomic counter subtract. See `incr`. Returns the new value. */
  decr(key: string, by = 1): number {
    return this.incr(key, -by);
  }

  /**
   * Export live entries for snapshots. Skips already-expired keys.
   * Values are shared by reference — stringify before storing.
   */
  dump(): SnapshotEntry[] {
    const now = Date.now();
    const out: SnapshotEntry[] = [];
    for (const [key, entry] of this.map) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= now) continue;
      if (entry.value === undefined) continue;
      out.push(
        entry.expiresAt === undefined
          ? { key, value: entry.value }
          : { key, value: entry.value, expiresAt: entry.expiresAt }
      );
    }
    return out;
  }

  /**
   * Restore entries from a snapshot (absolute `expiresAt` preserved).
   * Already-expired entries are skipped. Returns the number restored.
   */
  restore(entries: SnapshotEntry[]): number {
    const now = Date.now();
    let count = 0;
    for (const e of entries) {
      if (!e || typeof e.key !== 'string' || e.value === undefined) continue;
      if (e.expiresAt !== undefined && e.expiresAt <= now) continue;
      const old = this.map.get(e.key);
      if (old) {
        this.bytes -= old.size;
        this.map.delete(e.key);
      }
      const size = estimateSize(e.key, e.value);
      this.map.set(e.key, { value: e.value, expiresAt: e.expiresAt, size });
      this.bytes += size;
      count++;
    }
    while ((this.map.size > this.maxEntries || this.bytes > this.maxBytes) && this.map.size > 0) {
      const oldest = this.map.keys().next();
      if (oldest.done) break;
      const k = oldest.value as string;
      const en = this.map.get(k);
      if (!en) break;
      this.map.delete(k);
      this.bytes -= en.size;
      this.evictions++;
    }
    return count;
  }

  /** Ms remaining; -1 = persists; -2 = missing/expired. */
  ttl(key: string): number {
    const entry = this.map.get(key);
    if (!entry) {
      this.misses++;
      return -2;
    }
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      this.misses++;
      return -2;
    }
    if (entry.expiresAt === undefined) return -1;
    return Math.max(0, entry.expiresAt - Date.now());
  }

  /** Replace a key's TTL. Returns false if missing/expired. */
  expire(key: string, ttlMs: number): boolean {
    const ttl = validateTTL(ttlMs, 'ttlMs');
    const entry = this.map.get(key);
    if (!entry) return false;
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      return false;
    }
    entry.expiresAt = Date.now() + ttl;
    // LRU touch.
    this.map.delete(key);
    this.map.set(key, entry);
    return true;
  }

  /** Drop a key's TTL so it persists. Returns false if missing/expired. */
  persist(key: string): boolean {
    const entry = this.map.get(key);
    if (!entry) return false;
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      return false;
    }
    entry.expiresAt = undefined;
    this.map.delete(key);
    this.map.set(key, entry);
    return true;
  }

  /** Remove all expired keys. Returns the number removed. */
  sweep(): number {
    const now = Date.now();
    let count = 0;
    for (const [key, entry] of Array.from(this.map)) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= now) {
        this.map.delete(key);
        this.bytes -= entry.size;
        this.expiries++;
        count++;
      }
    }
    return count;
  }

  startSweeper(intervalMs?: number): void {
    const ms = intervalMs ?? this.sweepIntervalMs;
    if (!(ms > 0)) return;
    this.stopSweeper();
    this.timer = setInterval(() => {
      this.sweep();
    }, ms);
    // Don't hold the process open for a cache sweeper.
    const t = this.timer as unknown as { unref?: () => void };
    if (typeof t.unref === 'function') t.unref();
  }

  stopSweeper(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  /** Alias for stopSweeper (graceful shutdown). */
  close(): void {
    this.stopSweeper();
  }

  get size(): number {
    return this.map.size;
  }

  get bytesUsed(): number {
    return this.bytes;
  }

  stats(): KVStats {
    return {
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      expiries: this.expiries,
      entries: this.map.size,
      bytes: this.bytes,
    };
  }
}
