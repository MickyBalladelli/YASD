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

## P1 — Needed for production Echo (done)

- [x] Server mode for multi-instance (Socket.IO scaling) — `src/server.ts` (`YasdServer`), `src/protocol.ts` (RESP2 subset), `src/client.ts` (`YasdClient` pool + `CACHE_URL` + `fromEnv()`), `src/cli.ts` (`yasd-server` bin), `Dockerfile` + `docker-compose.yml` + `examples/server.js`
  - Standalone TCP/HTTP + simple RESP-like protocol, Node client with pool + `CACHE_URL`, healthcheck, graceful shutdown, `docker-compose` example.
  - Single port serves RESP + `GET /healthz`; graceful `close()` drains sockets, final SAVE, stops timers.
- [x] Persistence (optional for cache) — `src/persistence.ts`: `saveSnapshot`/`loadSnapshot` (atomic tmp+rename), `AofLog` append/replay/truncate, `SAVE`/`LOAD`, startup restore + periodic autosave + save-on-shutdown.
  - Snapshot + AOF/log, `SAVE`/`LOAD`, startup restore. Cache can be ephemeral but restart storm protection helps.
- [x] Atomic counters — `KVCache.incr/decr` (missing→0, TTL preserved, non-numeric throws), on `YASD` + protocol `INCR/DECR` + client.
  - `INCR/DECR` for rate limits, unread counts, like counts.
- [x] Pub/sub + invalidation — `src/pubsub.ts` (`PubSubHub`), on `YASD` (`publish/subscribe`) + protocol `PUBLISH/SUBSCRIBE/UNSUBSCRIBE` (dedicated multiplexed client conn); server auto-publishes mutations on `__yasd__:invalidate`.
  - Channels for cache invalidation, presence/typing, `PUBLISH/SUBSCRIBE`.
- [x] Pipelining / batch ops — `KVCache.mget/mset` (+ `YASD`, protocol `MGET/MSET`, client); TCP streaming + per-connection FIFO gives real pipelining.
  - `MGET/MSET`, batched writes for feed hydration.

## P2 — Nice to have

- [x] Transactions / CAS for read-modify-write.
  - Single-key: `KVCache.cas` (+ `YASD.cas`, protocol `CAS`, client `cas`).
  - Multi-key optimistic: per-key versions (`getVersion`, bumped on every
    mutation incl. expiry/eviction) + `KVCache.multi()` → `KVTransaction`
    (`watch/unwatch/get/set/mset/del/clearPrefix/expire/persist/incr/decr/cas`,
    `exec()` → results or null on conflict, `discard()`, commit-time
    validation + rollback for all-or-nothing) + `runTransaction(keys, fn)`
    retry helper on `KVCache`/`YASD`.
  - Server: `WATCH/UNWATCH/MULTI/EXEC/DISCARD` (KV ops + PING queueable,
    abort returns nil `*-1`, commits reuse the write path so AOF +
    invalidation fanout apply). Client: one-shot `YasdTransaction` on a
    dedicated connection (first write auto-sends MULTI, reads rejected after
    MULTI) + `YasdClient.runTransaction()`. Tests: `test/transactions.test.js`
    (21 tests, wired into `npm test`).
- [x] Metrics: hits/misses, evictions, expiries, memory bytes, slow-query log.
  - Counters: `KVCache.stats()` (`hits/misses/evictions/expiries` + live
    `entries`/`bytes`) surfaced as `YASD.cacheStats()`, server `INFO` +
    `/healthz`; `resetStats()` zeroes counters without dropping data.
  - Slow-query log: shared `SlowLog` in `src/metrics.ts` (threshold 0 = off,
    sub-ms precision, newest-first, capped at 100) backs the embedded SQL log
    (`slowQueryMs` option, `setSlowQueryThreshold/slowLog/clearSlowLog`) and
    the new server slow-command log (`slowCommandMs` option,
    `YASD_SLOW_COMMAND_MS` env, `--slow-command-ms` flag,
    `setSlowCommandThreshold/slowLog/clearSlowLog`, in `INFO`).
  - Tests: `test/metrics.test.js` (10 tests, wired into `npm test`).
  - Drive-by fix: unique tmp path per `saveSnapshot` call (periodic autosave
    + explicit SAVE + shutdown SAVE used to share one tmp file and could hit
    ENOENT on rename).
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
