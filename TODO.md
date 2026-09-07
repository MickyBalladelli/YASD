# YASD TODO — Become a good cache DB for Echo

Goal: replace/upgrade `Echo/server/src/cache/memory.js` (single-process `Map`, FIFO, `ttlMs=undefined → NaN → never expires`) with YASD as an embedded or server cache for hot feeds, channel lists, popular posts, presence/typing, rate limits, unread counts.

Echo seam to target: `cacheKey(namespace, value) / cacheGet(key) / cacheSet(key, value, ttlMs) / cacheClear(namespace)` used in `Echo/server/src/channels/service.js` and `Echo/server/src/posts/service.js`. Keep PostgreSQL as source of truth; YASD is cache only.

## P0 — Must have to beat `memory.js` (done)

- [x] Add KV fast path (no SQL on hot path) — `src/cache.ts` (`KVCache`), wired on `YASD` as `get/set/del/clearPrefix`
  - `get(key): value | undefined`, `set(key, value, ttlMs): value`, `del(key): boolean`, `clearPrefix(namespace): number`
  - Keep SQL for querying; do not force `SELECT * FROM cache WHERE key = ...` for feed caching (O(1) lookup required).
- [x] Add TTL + expiry — per-key `expiresAt`, lazy check on `get`, background sweeper (`sweepIntervalMs`, `unref`d)
  - Require `ttlMs` (no `undefined → NaN` bug — non-finite/negative throws), add `TTL(key)`, `EXPIRE(key, ttlMs)`, `PERSIST(key)` semantics.
  - Per-namespace defaults, e.g. feeds 10–30s, popular 60s, channel lists 30s.
- [x] Add real eviction + value types — `KVCache` is LRU (recency touch on get/set), `maxEntries` + `maxBytes`, size-aware insert
  - Extend `Value` in `src/types.ts` beyond `string | number | boolean | null` to support objects/arrays/JSON (Echo feeds are objects).
- [x] Use existing indexes on read — `planIndexLookup` in `src/executor.ts` serves `=` / `IN` (+ ANDs) from column indexes with full-scan fallback
  - Fix `DELETE` re-index loop (currently O(n²)) — replaced with single-pass `rebuildIndexes`.
- [x] Unify parser path — `Executor.execute()` now uses `parse()` from `src/parser.ts` only; the `parseSimple` duplicate is deleted (plus tokenizer fixes so the real parser actually works)

## P1 — Needed for production Echo

- [ ] Server mode for multi-instance (Socket.IO scaling)
  - Standalone TCP/HTTP + simple RESP-like protocol, Node client with pool + `CACHE_URL`, healthcheck, graceful shutdown, `docker-compose` example.
  - Without this YASD stays single-process (fine for 1-replica Echo only).
- [ ] Persistence (optional for cache)
  - Snapshot + AOF/log, `SAVE`/`LOAD`, startup restore. Cache can be ephemeral but restart storm protection helps.
- [ ] Atomic counters
  - `INCR/DECR` for rate limits, unread counts, like counts.
- [ ] Pub/sub + invalidation
  - Channels for cache invalidation, presence/typing, `PUBLISH/SUBSCRIBE`.
- [ ] Pipelining / batch ops
  - `MGET/MSET`, batched writes for feed hydration.

## P2 — Nice to have

- [ ] Transactions / CAS for read-modify-write.
- [ ] Metrics: hits/misses, evictions, expiries, memory bytes, slow-query log.
- [ ] Query profiling for `WHERE/ORDER BY/LIMIT` paths.
- [ ] Auth + TLS for server mode.

## Echo integration checklist

- [ ] Drop-in adapter in `Echo/server/src/cache/` implementing `cacheGet/cacheSet/cacheClear` on YASD KV API.
- [ ] Explicit invalidation on write (post/like/join/leave/read-state).
- [ ] Config: `CACHE_URL`, `CACHE_MAX_ENTRIES`, `CACHE_TTL_MS_*` via env.
- [ ] Load/chaos check: TTL expiry, LRU under pressure, restart behavior, multi-replica consistency.

## Non-goals

- Do not replace PostgreSQL. No joins across Echo domain tables, no durability guarantees for social data.
- Do not use SQL-string parsing on the per-request feed hot path.
