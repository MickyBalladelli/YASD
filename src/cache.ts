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
import {
  validateNonNegativeSafeInteger,
  validateNonNegativeNumber,
  validatePositiveSafeInteger,
} from './validation';

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
  /** Max total bytes (key + JSON value estimate). Oversized entries reject. Default 64 MiB. */
  maxBytes?: number;
  /** Fallback TTL (ms) when no namespace default matches. Default: persist. */
  defaultTTLMs?: number;
  /** Per-namespace TTL defaults (ms). Merged over DEFAULT_NAMESPACE_TTLS. */
  namespaceTTLMs?: Record<string, number>;
  /** Background sweep interval (ms). 0 disables. Default 1000. */
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

interface CacheState {
  // Keep the complete ordered map. A transaction can clear a prefix, create
  // new keys, and evict unrelated LRU entries before a later op fails.
  entries: Array<[string, Entry]>;
  bytes: number;
  hits: number;
  misses: number;
  evictions: number;
  expiries: number;
  keyVersions: Map<string, number>;
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

/**
 * Validate a runtime value as JSON data and return an owned deep copy.
 * TypeScript types do not protect JavaScript callers from undefined,
 * non-finite numbers, class instances, or cyclic objects.
 */
function cloneJsonValue(value: unknown, what: string): Value {
  const ancestors = new Set<object>();

  const clone = (current: unknown, path: string): Value => {
    if (current === null) return null;
    if (typeof current === 'string' || typeof current === 'boolean') return current;
    if (typeof current === 'number') {
      if (!Number.isFinite(current)) {
        throw new Error(`${what} must contain only finite JSON numbers at ${path}`);
      }
      return current;
    }
    if (typeof current === 'undefined') {
      throw new Error(`${what} cannot contain undefined at ${path} (use null)`);
    }
    if (typeof current !== 'object') {
      throw new Error(`${what} must contain only JSON values at ${path}`);
    }
    if (ancestors.has(current)) {
      throw new Error(`${what} cannot contain circular references at ${path}`);
    }

    ancestors.add(current);
    try {
      if (Array.isArray(current)) {
        if (Object.getPrototypeOf(current) !== Array.prototype) {
          throw new Error(`${what} must contain plain JSON arrays at ${path}`);
        }
        const out: Value[] = [];
        for (let i = 0; i < current.length; i++) {
          if (!Object.prototype.hasOwnProperty.call(current, i)) {
            throw new Error(`${what} cannot contain sparse arrays at ${path}[${i}]`);
          }
          out.push(clone(current[i], `${path}[${i}]`));
        }
        return out;
      }

      const prototype = Object.getPrototypeOf(current);
      if (prototype !== Object.prototype && prototype !== null) {
        throw new Error(`${what} must contain only plain JSON objects at ${path}`);
      }
      const out: { [key: string]: Value } = Object.create(null) as { [key: string]: Value };
      for (const key of Object.keys(current)) {
        out[key] = clone((current as Record<string, unknown>)[key], `${path}.${key}`);
      }
      return out;
    } finally {
      ancestors.delete(current);
    }
  };

  return clone(value, '$');
}

function validateTTL(ttlMs: number, what: string): number {
  return validateNonNegativeNumber(ttlMs, what);
}

export function validateKVOptions(options: KVOptions = {}): KVOptions {
  if (options === null || typeof options !== 'object' || Array.isArray(options)) {
    throw new Error('cache options must be an object');
  }
  const maxEntries = options.maxEntries === undefined
    ? 10_000
    : validatePositiveSafeInteger(options.maxEntries, 'maxEntries');
  const maxBytes = options.maxBytes === undefined
    ? 64 * 1024 * 1024
    : validatePositiveSafeInteger(options.maxBytes, 'maxBytes');
  const defaultTTLMs = options.defaultTTLMs === undefined
    ? undefined
    : validateTTL(options.defaultTTLMs, 'defaultTTLMs');
  const sweepIntervalMs = options.sweepIntervalMs === undefined
    ? 1000
    : validateNonNegativeNumber(options.sweepIntervalMs, 'sweepIntervalMs');
  const namespaceTTLMs: Record<string, number> = {
    ...DEFAULT_NAMESPACE_TTLS,
  };
  if (options.namespaceTTLMs !== undefined) {
    if (
      options.namespaceTTLMs === null ||
      typeof options.namespaceTTLMs !== 'object' ||
      Array.isArray(options.namespaceTTLMs)
    ) {
      throw new Error('namespaceTTLMs must be an object of finite TTL values');
    }
    Object.assign(namespaceTTLMs, options.namespaceTTLMs);
  }
  for (const [namespace, ttlMs] of Object.entries(namespaceTTLMs)) {
    namespaceTTLMs[namespace] = validateTTL(ttlMs, `namespaceTTLMs[${namespace}]`);
  }
  return {
    maxEntries,
    maxBytes,
    ...(defaultTTLMs === undefined ? {} : { defaultTTLMs }),
    namespaceTTLMs,
    sweepIntervalMs,
  };
}

export type KVExpiryListener = (key: string) => void

export class KVCache {
  private map = new Map<string, Entry>();
  private bytes = 0;
  private hits = 0;
  private misses = 0;
  private evictions = 0;
  private expiries = 0;
  private timer?: ReturnType<typeof setInterval>;
  private expiryListener?: KVExpiryListener
  private atomicFrames: Array<{ state: CacheState; expiredKeys: Set<string> }> = []
  /**
   * Per-key mutation generation for optimistic transactions (WATCH).
   * Bumped on every state change (writes, deletes, TTL changes, expiry).
   * `getVersion` returns 0 for never-written keys. Tombstones are kept so a
   * delete is still observed as a change; the map is pruned (safe direction:
   * pruning can only cause a false abort, never a missed conflict).
   */
  private keyVersions = new Map<string, number>();

  readonly maxEntries: number;
  readonly maxBytes: number;
  readonly defaultTTLMs?: number;
  readonly namespaceTTLMs: Record<string, number>;
  readonly sweepIntervalMs: number;

  constructor(options: KVOptions = {}, expiryListener?: KVExpiryListener) {
    const validated = validateKVOptions(options);
    this.maxEntries = validated.maxEntries as number;
    this.maxBytes = validated.maxBytes as number;
    this.defaultTTLMs = validated.defaultTTLMs;
    this.namespaceTTLMs = validated.namespaceTTLMs as Record<string, number>;
    this.sweepIntervalMs = validated.sweepIntervalMs as number;
    this.expiryListener = expiryListener
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

  private notifyExpired(key: string): void {
    const frame = this.atomicFrames[this.atomicFrames.length - 1]
    if (frame) {
      frame.expiredKeys.add(key)
      return
    }
    try {
      this.expiryListener?.(key)
    } catch {
      // Expiry observers must not break cache mutation.
    }
  }

  private flushExpiryEvents(keys: Set<string>): void {
    if (!this.expiryListener) return
    for (const key of keys) {
      try {
        this.expiryListener(key)
      } catch {
        // Expiry observers must not break cache mutation.
      }
    }
  }

  private removeExpired(key: string, entry: Entry): void {
    this.map.delete(key);
    this.bytes -= entry.size;
    this.expiries++;
    this.bumpVersion(key);
    this.notifyExpired(key)
  }

  private captureState(): CacheState {
    const entries: Array<[string, Entry]> = [];
    for (const [key, entry] of this.map) {
      entries.push([
        key,
        {
          value: cloneJsonValue(entry.value, 'cache value'),
          expiresAt: entry.expiresAt,
          size: entry.size,
        },
      ]);
    }
    return {
      entries,
      bytes: this.bytes,
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      expiries: this.expiries,
      keyVersions: new Map(this.keyVersions),
    };
  }

  private restoreState(state: CacheState): void {
    this.map = new Map(state.entries);
    this.bytes = state.bytes;
    this.hits = state.hits;
    this.misses = state.misses;
    this.evictions = state.evictions;
    this.expiries = state.expiries;
    this.keyVersions = new Map(state.keyVersions);
  }

  /** Run synchronous work atomically, restoring all cache state on failure. */
  atomic<T>(fn: () => T): T {
    const frame = { state: this.captureState(), expiredKeys: new Set<string>() }
    this.atomicFrames.push(frame)
    try {
      const result = fn()
      this.atomicFrames.pop()
      const parent = this.atomicFrames[this.atomicFrames.length - 1]
      if (parent) {
        for (const key of frame.expiredKeys) parent.expiredKeys.add(key)
      } else {
        this.flushExpiryEvents(frame.expiredKeys)
      }
      return result
    } catch (err) {
      this.atomicFrames.pop()
      this.restoreState(frame.state)
      throw err;
    }
  }

  /**
   * Mutation generation for `key` (0 = never written). Used by
   * `KVTransaction` (WATCH) and the server's WATCH command to detect
   * read-modify-write conflicts between watch time and commit time.
   */
  getVersion(key: string): number {
    return this.keyVersions.get(key) ?? 0;
  }

  private bumpVersion(key: string): void {
    this.keyVersions.set(key, (this.keyVersions.get(key) ?? 0) + 1);
    // Bound the tombstone map: dropping versions for non-live keys can only
    // cause a false transaction abort (version reads as 0, mismatching any
    // earlier snapshot), never a missed conflict.
    if (this.keyVersions.size > this.maxEntries * 2 + 1024) {
      for (const k of this.keyVersions.keys()) {
        if (!this.map.has(k)) this.keyVersions.delete(k);
        if (this.keyVersions.size <= this.maxEntries + 512) break;
      }
    }
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
    return cloneJsonValue(entry.value, 'cached value');
  }

  /**
   * O(1) insert (amortized). Size-aware LRU eviction: after insert, evict
   * least-recently-used entries while over `maxEntries`/`maxBytes`.
   * A single entry larger than `maxBytes` is rejected.
   */
  set(key: string, value: Value, ttlMs?: number): Value {
    const ownedValue = cloneJsonValue(value, 'cache value');
    const ttl = this.resolveTTLMs(key, ttlMs);
    const expiresAt = ttl === undefined ? undefined : Date.now() + ttl;
    return this.setOwned(key, ownedValue, expiresAt, value);
  }

  /** Store a value with an absolute expiry deadline (used by AOF replay). */
  setAt(key: string, value: Value, expiresAt?: number): Value {
    if (expiresAt !== undefined && !Number.isFinite(expiresAt)) {
      throw new Error(`expiresAt must be a finite epoch ms, got ${String(expiresAt)}`);
    }
    const ownedValue = cloneJsonValue(value, 'cache value');
    return this.setOwned(key, ownedValue, expiresAt, value);
  }

  private setOwned(key: string, ownedValue: Value, expiresAt: number | undefined, returned: Value): Value {
    const size = estimateSize(key, ownedValue);
    this.assertEntryFits(key, size);
    // Immediate expiry (ttl 0): behave like a write-through miss.
    if (expiresAt !== undefined && expiresAt <= Date.now()) {
      const old = this.map.get(key);
      if (old) {
        this.map.delete(key);
        this.bytes -= old.size;
        this.bumpVersion(key);
      }
      this.expiries++;
      this.misses++;
      return returned;
    }
    const old = this.map.get(key);
    if (old) {
      this.bytes -= old.size;
      this.map.delete(key);
    }
    this.map.set(key, { value: ownedValue, expiresAt, size });
    this.bytes += size;
    this.bumpVersion(key);
    this.evictIfNeeded();
    return returned;
  }

  private assertEntryFits(key: string, size: number): void {
    if (size > this.maxBytes) {
      throw new Error(`cache entry '${key}' exceeds maxBytes (${size} > ${this.maxBytes})`);
    }
  }

  private evictIfNeeded(): void {
    while ((this.map.size > this.maxEntries || this.bytes > this.maxBytes) && this.map.size > 0) {
      const oldest = this.map.keys().next();
      if (oldest.done) break;
      const oldestKey = oldest.value as string;
      const entry = this.map.get(oldestKey);
      if (!entry) break;
      // Skip expired entries via accounting as expiries, not evictions.
      if (this.isExpired(entry, Date.now())) {
        this.removeExpired(oldestKey, entry);
      } else {
        this.map.delete(oldestKey);
        this.bytes -= entry.size;
        this.evictions++;
        this.bumpVersion(oldestKey);
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
    this.bumpVersion(key);
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
        this.bumpVersion(key);
        count++;
      }
    }
    return count;
  }

  clear(): void {
    for (const key of this.map.keys()) this.bumpVersion(key);
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
    if (!Array.isArray(entries)) throw new Error('mset entries must be an array');
    const validated = entries.map(e => {
      if (!e || typeof e.key !== 'string') {
        throw new Error('mset entries must be { key: string, value, ttlMs? }');
      }
      const value = cloneJsonValue(e.value, 'mset value');
      if (e.ttlMs !== undefined) validateTTL(e.ttlMs, 'ttlMs');
      this.assertEntryFits(e.key, estimateSize(e.key, value));
      return { key: e.key, value, ttlMs: e.ttlMs };
    });
    for (const e of validated) {
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
    const size = estimateSize(key, next);
    this.assertEntryFits(key, size);
    const live = this.map.get(key);
    if (live && !this.isExpired(live, Date.now())) {
      this.bytes -= live.size;
      this.map.delete(key);
    }
    this.map.set(key, { value: next, expiresAt, size });
    this.bytes += size;
    this.bumpVersion(key);
    this.evictIfNeeded();
    return next;
  }

  /** Atomic counter subtract. See `incr`. Returns the new value. */
  decr(key: string, by = 1): number {
    return this.incr(key, -by);
  }

  private static valuesEqual(a: Value | undefined, b: Value | undefined): boolean {
    if (a === b) return true;
    if (a === undefined || b === undefined) return false;
    if (typeof a === 'object' && a !== null && typeof b === 'object' && b !== null) {
      try {
        return JSON.stringify(a) === JSON.stringify(b);
      } catch {
        return false;
      }
    }
    return false;
  }

  /**
   * Compare-and-set for read-modify-write (distributed locks, single-flight
   * hydration, exactly-once counters). Succeeds when the live value
   * deep-equals `expected` — pass `undefined` to assert absence (expired
   * counts as absent). On success stores `value` and returns true, otherwise
   * state is untouched and it returns false. Single-threaded execution makes
   * the check-and-store uninterrupted.
   *
   * TTL: an explicit `ttlMs` wins; otherwise the key's existing TTL is
   * preserved; new keys fall back to namespace/default resolution (as `set`).
   */
  cas(key: string, expected: Value | undefined, value: Value, ttlMs?: number): boolean {
    const ownedExpected = expected === undefined ? undefined : cloneJsonValue(expected, 'cas expected');
    const ownedValue = cloneJsonValue(value, 'cas value');
    const size = estimateSize(key, ownedValue);
    this.assertEntryFits(key, size);
    const now = Date.now();
    const entry = this.map.get(key);
    let current: Value | undefined;
    let keepExpiresAt: number | undefined;
    if (entry) {
      if (this.isExpired(entry, now)) {
        this.removeExpired(key, entry);
        current = undefined;
      } else {
        current = entry.value;
        keepExpiresAt = entry.expiresAt;
      }
    }
    if (!KVCache.valuesEqual(current, ownedExpected)) return false;
    const resolvedDefault = ttlMs !== undefined ? undefined : this.resolveTTLMs(key, undefined);
    const expiresAt =
      ttlMs !== undefined
        ? now + validateTTL(ttlMs, 'ttlMs')
        : keepExpiresAt !== undefined
          ? keepExpiresAt
          : resolvedDefault === undefined
            ? undefined
            : now + resolvedDefault;
    if (expiresAt !== undefined && expiresAt <= now) {
      if (entry && this.map.get(key) === entry) {
        this.map.delete(key);
        this.bytes -= entry.size;
        this.bumpVersion(key);
      }
      this.expiries++;
      return true; // compare succeeded; ttl 0 means "don't keep it"
    }
    const live = this.map.get(key);
    if (live) {
      this.bytes -= live.size;
      this.map.delete(key);
    }
    this.map.set(key, { value: ownedValue, expiresAt, size });
    this.bytes += size;
    this.bumpVersion(key);
    this.evictIfNeeded();
    return true;
  }

  /**
   * Export live entries for snapshots. Skips already-expired keys.
   * Values are cloned so mutating a snapshot cannot mutate the cache.
   */
  dump(): SnapshotEntry[] {
    const now = Date.now();
    const out: SnapshotEntry[] = [];
    for (const [key, entry] of this.map) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= now) continue;
      if (entry.value === undefined) continue;
      out.push(
        entry.expiresAt === undefined
          ? { key, value: cloneJsonValue(entry.value, 'snapshot value') }
          : { key, value: cloneJsonValue(entry.value, 'snapshot value'), expiresAt: entry.expiresAt }
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
    const pending: Array<{ entry: SnapshotEntry; value: Value }> = [];
    for (const e of entries) {
      if (!e || typeof e.key !== 'string' || e.value === undefined) continue;
      if (e.expiresAt !== undefined && e.expiresAt <= now) continue;
      const value = cloneJsonValue(e.value, 'snapshot value');
      this.assertEntryFits(e.key, estimateSize(e.key, value));
      pending.push({ entry: e, value });
    }
    let count = 0;
    for (const { entry: e, value } of pending) {
      const old = this.map.get(e.key);
      if (old) {
        this.bytes -= old.size;
        this.map.delete(e.key);
      }
      const size = estimateSize(e.key, value);
      this.map.set(e.key, { value, expiresAt: e.expiresAt, size });
      this.bytes += size;
      this.bumpVersion(e.key);
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
      this.bumpVersion(k);
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

  /** Absolute expiry deadline; undefined = persistent, null = missing/expired. */
  expiration(key: string): number | undefined | null {
    const entry = this.map.get(key);
    if (!entry) return null;
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      return null;
    }
    return entry.expiresAt;
  }

  /** Apply an absolute expiry deadline without extending it during replay. */
  expireAt(key: string, expiresAt: number): boolean {
    if (!Number.isFinite(expiresAt)) {
      throw new Error(`expiresAt must be a finite epoch ms, got ${String(expiresAt)}`);
    }
    const entry = this.map.get(key);
    if (!entry) return false;
    if (this.isExpired(entry, Date.now())) {
      this.removeExpired(key, entry);
      return false;
    }
    if (expiresAt <= Date.now()) {
      this.map.delete(key);
      this.bytes -= entry.size;
      this.expiries++;
      this.bumpVersion(key);
      this.notifyExpired(key)
      return true;
    }
    entry.expiresAt = expiresAt;
    this.map.delete(key);
    this.map.set(key, entry);
    this.bumpVersion(key);
    return true;
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
    this.bumpVersion(key);
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
    this.bumpVersion(key);
    return true;
  }

  /** Remove all expired keys. Returns the number removed. */
  sweep(): number {
    const now = Date.now();
    let count = 0;
    for (const [key, entry] of Array.from(this.map)) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= now) {
        this.removeExpired(key, entry)
        count++;
      }
    }
    return count;
  }

  startSweeper(intervalMs?: number): void {
    const ms = intervalMs === undefined
      ? this.sweepIntervalMs
      : validateNonNegativeNumber(intervalMs, 'sweepIntervalMs');
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

  /**
   * Start an optimistic transaction (Redis-style WATCH/MULTI/EXEC).
   * Queue writes on the returned object, then `exec()` to commit them
   * atomically — single-threaded application makes the commit
   * uninterrupted, and any watched key mutated since `watch()` aborts the
   * commit (returns null) instead of applying a stale read-modify-write.
   */
  multi(): KVTransaction {
    return new KVTransaction(this);
  }

  /**
   * Watch-then-commit loop with retries. Watches `keys`, runs `fn(tx)`
   * (read via `tx.get`, queue writes via `tx.set/del/incr/...`), commits,
   * and retries on conflict up to `maxRetries` times. A throw inside `fn`
   * discards and rethrows; `tx.discard()` inside `fn` aborts voluntarily
   * (`committed: false`, no retry).
   */
  async runTransaction<T>(
    keys: string[],
    fn: (tx: KVTransaction) => T | Promise<T>,
    maxRetries = 3
  ): Promise<{ committed: boolean; attempts: number; results: TxResult[] | null; value: T | undefined }> {
    validateNonNegativeSafeInteger(maxRetries, 'maxRetries');
    let attempts = 0;
    let value: T | undefined;
    for (;;) {
      const tx = this.multi();
      tx.watch(...keys);
      attempts++;
      value = await fn(tx);
      if (tx.finished) {
        return { committed: false, attempts, results: null, value };
      }
      const results = tx.exec();
      if (results !== null) {
        return { committed: true, attempts, results, value };
      }
      if (attempts > maxRetries) {
        return { committed: false, attempts, results: null, value };
      }
    }
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

  /**
   * Zero the cumulative counters (hits/misses/evictions/expiries).
   * Live entries and their bytes are untouched — use `clear()` to drop data.
   */
  resetStats(): void {
    this.hits = 0;
    this.misses = 0;
    this.evictions = 0;
    this.expiries = 0;
  }
}

/** Per-op result of a committed transaction (mirrors the queued method). */
export type TxResult = Value | boolean | number;

type TxQueuedOp =
  | { op: 'set'; key: string; value: Value; ttlMs?: number }
  | { op: 'mset'; entries: KVBatchEntry[] }
  | { op: 'del'; key: string }
  | { op: 'clearPrefix'; prefix: string }
  | { op: 'expire'; key: string; ttlMs: number }
  | { op: 'persist'; key: string }
  | { op: 'incr'; key: string; by: number }
  | { op: 'decr'; key: string; by: number }
  | { op: 'cas'; key: string; expected: Value | undefined; value: Value; ttlMs?: number };

export class TransactionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransactionError';
  }
}

/**
 * Optimistic multi-key transaction over a KVCache (Redis-style).
 *
 *   const tx = cache.multi();
 *   tx.watch('likes:1', 'feed:home');   // snapshot versions
 *   const likes = tx.get('likes:1');     // immediate read (post-watch)
 *   tx.set('likes:1', likes + 1);        // queued write
 *   tx.incr('feed:home:version');        // queued write
 *   const results = tx.exec();           // null = watched key changed, abort
 *
 * Reads (`get`) execute immediately against live state; writes queue and
 * apply in order inside `exec()`, which is synchronous and therefore atomic
 * on Node's single thread. `exec()` validates every op before mutating, and
 * rolls back (restores pre-commit snapshots) if a commit-time check fails,
 * so a commit is all-or-nothing. One-shot: `exec()`/`discard()` finish the
 * transaction; any further use throws.
 */
export class KVTransaction {
  private watched = new Map<string, number>();
  private queue: TxQueuedOp[] = [];
  private done = false;

  constructor(private readonly cache: KVCache) {}

  /** True once `exec()` or `discard()` has run. */
  get finished(): boolean {
    return this.done;
  }

  /** Number of queued writes. */
  get queued(): number {
    return this.queue.length;
  }

  /** Snapshot versions of `keys`; aborts `exec()` if any change since. */
  watch(...keys: string[]): this {
    this.assertOpen('watch');
    for (const key of keys) {
      if (typeof key !== 'string') throw new TransactionError('watch keys must be strings');
      this.watched.set(key, this.cache.getVersion(key));
    }
    return this;
  }

  /** Forget all watched versions (EXEC still commits queued writes). */
  unwatch(): this {
    this.assertOpen('unwatch');
    this.watched.clear();
    return this;
  }

  /**
   * Immediate read against live state (lazy expiry + LRU touch, like `get`).
   * Call after `watch(key)` for the classic read-modify-write pattern.
   */
  get(key: string): Value | undefined {
    this.assertOpen('get');
    return this.cache.get(key);
  }

  /** Queue a SET (same TTL resolution/validation as `KVCache.set`). */
  set(key: string, value: Value, ttlMs?: number): this {
    this.assertOpen('set');
    const ownedValue = KVTransaction.checkValue(value, 'set');
    KVTransaction.checkTtl(ttlMs);
    this.queue.push({ op: 'set', key, value: ownedValue, ttlMs });
    return this;
  }

  /** Queue an MSET batch. */
  mset(entries: KVBatchEntry[]): this {
    this.assertOpen('mset');
    if (!Array.isArray(entries)) throw new TransactionError('mset entries must be an array');
    const ownedEntries: KVBatchEntry[] = [];
    for (const e of entries) {
      if (!e || typeof e.key !== 'string') {
        throw new TransactionError('mset entries must be { key: string, value, ttlMs? }');
      }
      const value = KVTransaction.checkValue(e.value, 'mset');
      KVTransaction.checkTtl(e.ttlMs);
      ownedEntries.push({ key: e.key, value, ttlMs: e.ttlMs });
    }
    this.queue.push({ op: 'mset', entries: ownedEntries });
    return this;
  }

  /** Queue a DEL. Result at commit: true when a live key was removed. */
  del(key: string): this {
    this.assertOpen('del');
    this.queue.push({ op: 'del', key });
    return this;
  }

  /** Queue a namespace clear. Result at commit: number of keys removed. */
  clearPrefix(prefix: string): this {
    this.assertOpen('clearPrefix');
    if (typeof prefix !== 'string') throw new TransactionError('clearPrefix prefix must be a string');
    this.queue.push({ op: 'clearPrefix', prefix });
    return this;
  }

  /** Queue an EXPIRE. Result at commit: false when missing/expired. */
  expire(key: string, ttlMs: number): this {
    this.assertOpen('expire');
    KVTransaction.checkTtl(ttlMs, true);
    this.queue.push({ op: 'expire', key, ttlMs });
    return this;
  }

  /** Queue a PERSIST. Result at commit: false when missing/expired. */
  persist(key: string): this {
    this.assertOpen('persist');
    this.queue.push({ op: 'persist', key });
    return this;
  }

  /** Queue an INCR. Fails the commit when the live value is non-numeric. */
  incr(key: string, by = 1): this {
    this.assertOpen('incr');
    KVTransaction.checkDelta(by);
    this.queue.push({ op: 'incr', key, by });
    return this;
  }

  /** Queue a DECR. */
  decr(key: string, by = 1): this {
    this.assertOpen('decr');
    KVTransaction.checkDelta(by);
    this.queue.push({ op: 'decr', key, by });
    return this;
  }

  /** Queue a CAS. Result at commit: true on compare success. */
  cas(key: string, expected: Value | undefined, value: Value, ttlMs?: number): this {
    this.assertOpen('cas');
    const ownedExpected = expected === undefined ? undefined : KVTransaction.checkValue(expected, 'cas expected');
    const ownedValue = KVTransaction.checkValue(value, 'cas');
    KVTransaction.checkTtl(ttlMs);
    this.queue.push({ op: 'cas', key, expected: ownedExpected, value: ownedValue, ttlMs });
    return this;
  }

  /** Drop queued writes and watches; the transaction is finished. */
  discard(): void {
    this.assertOpen('discard');
    this.queue = [];
    this.watched.clear();
    this.done = true;
  }

  /**
   * Commit queued writes atomically. Returns per-op results in queue order,
   * or null when a watched key changed since `watch()` (nothing applied).
   * Throws `TransactionError` when a commit-time check fails (nothing
   * applied — pre-commit snapshots are restored) or the transaction is
   * already finished.
   */
  exec(): TxResult[] | null {
    this.assertOpen('exec');
    this.done = true;
    for (const [key, version] of this.watched) {
      if (this.cache.getVersion(key) !== version) {
        this.queue = [];
        this.watched.clear();
        return null;
      }
    }
    const ops = this.queue;
    this.queue = [];
    this.watched.clear();
    if (ops.length === 0) return [];
    return this.cache.atomic(() => {
      const results: TxResult[] = [];
      for (const op of ops) results.push(this.apply(op));
      return results;
    });
  }

  // ---- internals ----

  private assertOpen(what: string): void {
    if (this.done) throw new TransactionError(`cannot ${what}: transaction already finished (exec/discard)`);
  }

  private static checkValue(value: Value | undefined, what: string): Value {
    if (value === undefined) throw new TransactionError(`${what} value cannot be undefined (use null)`);
    try {
      return cloneJsonValue(value, `${what} value`);
    } catch (err) {
      throw new TransactionError(err instanceof Error ? err.message : String(err));
    }
  }

  private static checkTtl(ttlMs: number | undefined, required = false): void {
    if (ttlMs === undefined) {
      if (required) throw new TransactionError('ttlMs is required');
      return;
    }
    try {
      validateNonNegativeNumber(ttlMs, 'ttlMs');
    } catch (err) {
      throw new TransactionError((err as Error).message);
    }
  }

  private static checkDelta(by: number): void {
    if (typeof by !== 'number' || !Number.isFinite(by)) {
      throw new TransactionError(`delta must be a finite number, got ${String(by)}`);
    }
  }

  private apply(op: TxQueuedOp): TxResult {
    switch (op.op) {
      case 'set':
        return this.cache.set(op.key, op.value, op.ttlMs);
      case 'mset':
        return this.cache.mset(op.entries);
      case 'del':
        return this.cache.del(op.key);
      case 'clearPrefix':
        return this.cache.clearPrefix(op.prefix);
      case 'expire':
        return this.cache.expire(op.key, op.ttlMs);
      case 'persist':
        return this.cache.persist(op.key);
      case 'incr':
        return this.cache.incr(op.key, op.by);
      case 'decr':
        return this.cache.decr(op.key, op.by);
      case 'cas':
        return this.cache.cas(op.key, op.expected, op.value, op.ttlMs);
    }
  }
}
