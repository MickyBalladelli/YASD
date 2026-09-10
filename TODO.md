# YASD improvement TODO

Review focus: SQL correctness, KV cache behavior, server safety, persistence,
client reliability, tests, and docs. Line references point to the current
source.

## P0 — Fix correctness and safety first

- [x] Make cached JSON values safe to own. `KVCache.set()` stores object
  references and `get()` returns them (`src/cache.ts:191-207,214-239`). A caller
  can mutate a value without updating bytes, LRU order, versions, or
  invalidation. Validate JSON values at the API boundary and either deep-clone
  on read/write or clearly enforce immutable values.
- [x] Make embedded and server transactions truly all-or-nothing. Server
  `EXEC` applies each command and catches errors after earlier commands already
  changed state (`src/server.ts:616-641`). It also writes AOF entries and
  publishes invalidations during the partial commit. Plan/validate first, then
  commit once, with one transaction-level persistence and fanout step.
- [x] Fix transaction rollback coverage. Embedded rollback snapshots only keys
  found before `clearPrefix()` and can leave newly created matching keys behind
  after a later failure (`src/cache.ts:899-939`). Preserve the full affected
  namespace and restore data, TTL, LRU order, and relevant counters exactly.
- [x] Make snapshot/AOF recovery crash-safe. A crash after snapshot rename but
  before AOF truncation can replay old `INCR` operations on top of the snapshot
  and double-count (`src/persistence.ts:42-53,158-163`). Use AOF generation or
  offset metadata plus atomic log rotation and a recovery test.
- [x] Persist TTL deadlines, not only relative TTLs. AOF replay currently starts
  each TTL from replay time (`src/persistence.ts:82-105`), extending entries
  after a restart. Store `expiresAt` or an operation timestamp and preserve the
  original deadline.
- [x] Fix CAS persistence with `PX 0`. A successful immediate-expiry CAS is
  logged as a normal `set` without a TTL because the key is already gone
  (`src/server.ts:664-686`), so restart can resurrect it. Log the exact expiry
  result or omit the operation when it leaves no key.
- [x] Do not hide AOF write failures. `YasdServer.logAof()` swallows all errors
  (`src/server.ts:536-542`) even though the command is acknowledged. Surface a
  write failure, expose degraded persistence in `INFO`, and define whether the
  server rejects or serves writes when durability is unavailable.
- [x] Add hard limits to RESP decoding. `RespDecoder` grows its buffer and
  recursively parses arrays without limits (`src/protocol.ts:79-151`). Enforce
  maximum frame bytes, argument count, bulk length, nesting depth, and buffered
  bytes; close abusive connections cleanly.
- [x] Make the default server safer. The default host is loopback. Explicit
  public binds without auth/TLS print a clear startup warning.

## P1 — Server, cache, and client reliability

- [x] Serialize `save()` and `load()` operations. Autosave, explicit SAVE, and
  shutdown now share one persistence queue; concurrent calls are covered by a
  regression test.
- [x] Treat AOF corruption deliberately. Replay now stops at middle corruption,
  tolerates only an incomplete final line, and exposes recovery state in INFO.
- [x] Publish every state mutation. `PERSIST`, background expiry, and `LOAD`
  now publish invalidation events for local replica caches.
- [ ] Prevent public bypasses of server hooks. `YasdServer.cache` exposes the
  raw `KVCache` (`src/server.ts:259-262`), so callers can mutate data without
  AOF logging or invalidation. Return a controlled facade or make the raw cache
  private.
- [ ] Validate all configuration inputs consistently. `parseInt()` accepts
  values such as `10oops`, and direct constructor options can be `NaN`, zero,
  or negative (`src/server.ts:135-180`, `src/cache.ts:126-134`). Add shared
  strict parsers and reject invalid `maxEntries`, `maxBytes`, TTLs, ports,
  timeouts, and autosave intervals.
- [ ] Enforce cache limits exactly. With `maxEntries <= 0` or `maxBytes <= 0`,
  eviction deliberately keeps one entry (`src/cache.ts:242-267`). Validate
  positive limits and define whether an oversized single value is rejected or
  allowed.
- [ ] Make byte accounting use UTF-8 for keys as well as values. The current
  estimate uses `key.length * 2` (`src/cache.ts:76-90`) while the public metric
  is called bytes. Use one documented size model and test non-ASCII keys.
- [ ] Add backpressure and output limits. Server writes ignore the return value
  of `socket.write()` (`src/server.ts:524-526,796-855`), so a slow subscriber can
  grow memory without bound. Queue bounded output, pause reads, or disconnect
  slow consumers.
- [ ] Improve client pool recovery. Dead connections remain in `pool` until a
  full-pool reset (`src/client.ts:514-527,623-635`), so repeated partial failures
  can grow stale entries. Remove dead connections and reconnect with bounded
  backoff.
- [ ] Make subscription state machine robust. Subscribe/unsubscribe calls can
  overlap, ack timeout cleanup is incomplete, and reconnect is only attempted
  when another subscribe call happens (`src/client.ts:449-487,642-792`). Serialize
  channel commands, clean all waiter/timer state, and offer explicit reconnect
  or resubscribe behavior.
- [ ] Add input validation and limits to `parseCacheUrl()` and client options.
  Validate host, port, timeout, pool size, URL decoding, and TLS combinations in
  one place (`src/client.ts:44-141`).
- [ ] Separate the HTTP health endpoint from RESP detection, or make detection
  fully incremental. `src/server.ts:438-447` only recognizes `GET ` when the
  first TCP chunk already contains four bytes. Partial HTTP writes are treated
  as RESP and dropped. Also parse method, path, query, and HTTP version strictly.
- [ ] Decide what health data may be public. `/healthz` exposes channels,
  counters, TLS/auth flags, and slow-command details without auth
  (`src/server.ts:456-492`). Add a minimal liveness endpoint, a readiness
  endpoint, and optional protection/redaction for operational data.

## P1 — SQL correctness and predictable semantics

- [ ] Preserve literal-vs-column identity in the AST. Both quoted strings and
  identifiers become `string`; execution then treats a string as a column when
  a same-named column exists (`src/parser.ts:188-229`,
  `src/executor.ts:572-589`). For example, `name = 'age'` can compare against
  the `age` column. Add explicit literal and column-reference AST nodes.
- [ ] Reject trailing SQL instead of silently ignoring it. Parser entry points
  consume an optional semicolon but never require end-of-input
  (`src/parser.ts:232-251,585-588`). Reject extra tokens and support only one
  statement unless a deliberate script API is added.
- [ ] Harden tokenization. Detect unterminated quotes, support documented SQL
  escaping, make `NULL/TRUE/FALSE` case-insensitive, and reject malformed
  numeric tokens. Replace parser `any` values with typed AST values.
- [ ] Reject unknown types and invalid schemas. `parseType()` silently maps an
  unknown type to `any` (`src/parser.ts:162-185`). Validate duplicate columns,
  empty tables, primary-key existence, and multiple primary-key declarations.
- [ ] Enforce constraints on every write. Validate row arity, duplicate INSERT
  columns, supplied `NULL` for `NOT NULL`, type conversion failures, primary-key
  uniqueness, and primary-key updates (`src/executor.ts:253-343,419-487`).
- [ ] Make writes atomic on validation failure. Multi-row INSERT and UPDATE can
  modify earlier rows before a later row/clause fails. Validate the complete
  statement first or roll it back on error.
- [ ] Define NULL behavior and implement it consistently. Current comparisons
  return `true` for many `NULL != value` cases (`src/executor.ts:591-604`) and
  there is no `IS NULL`/`IS NOT NULL`. Choose SQL-like three-valued behavior or
  explicitly document the simpler rules and test them.
- [ ] Validate referenced columns. Unknown projection and ORDER BY columns are
  silently returned/sorted as `null` (`src/executor.ts:372-410`). Return a clear
  `COLUMN_NOT_FOUND` error instead.
- [ ] Fix or remove unsupported SQL examples. `examples/basic.js:75-77` uses
  `COUNT(*) AS count`, but the parser has no aggregate or alias support. Either
  implement aggregates/aliases or change the example to count returned rows.
- [ ] Fix ORDER BY NULL ordering and type mismatch behavior. Null placement
  changes with ASC/DESC because the comparator applies direction after its null
  result (`src/executor.ts:372-389`). Define stable ordering for nulls and
  incompatible types.
- [ ] Improve indexed query planning. UPDATE/DELETE still scan all rows, every
  column is indexed by default, and index lists use linear removal
  (`src/executor.ts:234-242,419-477`). Add range indexes or better plans where
  useful, make index creation configurable, and benchmark memory/write costs.

## P2 — API, packaging, and maintainability

- [ ] Fix the public constructor type. `YASD` accepts `KVOptions`, then casts to
  `YasdOptions` to read `slowQueryMs` (`src/index.ts:64-91`). Type it as
  `YasdOptions` so the documented `new YASD({ slowQueryMs: 5 })` compiles.
- [ ] Export a stable typed database error and error-code list. The executor's
  `DatabaseError` is private (`src/executor.ts:28-33`) while consumers need to
  distinguish table, column, constraint, parse, and protocol failures.
- [ ] Remove or justify the import-time singleton. `src/index.ts:377-379` creates
  a global database and sweeper for every import. Prefer an explicit factory or
  document the shared mutable default and its lifecycle.
- [ ] Make JSON equality deterministic and safe. `JSON.stringify()` comparison
  depends on object insertion order (`src/cache.ts:378-389`,
  `src/executor.ts:559-569`). Use structural equality or canonical JSON, and
  test nested values, key order, and large objects.
- [ ] Add package metadata and an explicit package surface: supported Node
  versions, repository/homepage, exports for CommonJS/types, included files,
  and a version source shared by `package.json`, server INFO, and docs.
- [ ] Add graceful operational controls: max key/value size, max command time,
  idle connection timeout, shutdown deadline, and a way to inspect persistence
  errors without exposing sensitive values.

## P2 — Tests and delivery

- [ ] Make test execution fail when the build is missing. Several scripts exit
  successfully after printing “skipping tests” when `dist` is unavailable
  (`test/basic.test.js:4-24`, `test/kv.test.js:5-13`). Build in the test script
  or use a runner that reports missing prerequisites as failures.
- [ ] Add regression tests for every P0/P1 item: literal-vs-column parsing,
  trailing tokens, constraints, atomic failed writes, mutable cache values,
  CAS `PX 0`, TTL-preserving restart, snapshot/AOF crash windows, corrupted
  AOF lines, RESP limits, partial HTTP headers, PERSIST invalidation, expiry
  invalidation, client reconnects, and slow-consumer backpressure.
- [ ] Add property/fuzz tests for the SQL tokenizer/parser and RESP decoder.
  Include random chunk boundaries, malformed lengths, deep arrays, invalid
  UTF-8, quotes, numbers, and very large input.
- [ ] Add concurrency and load benchmarks for LRU, expiry sweeping, indexed
  reads, UPDATE/DELETE, pub/sub fanout, pooled client traffic, and persistence.
  Track latency percentiles and memory, not only operation counts.
- [ ] Add CI that runs build, tests, type checks, packaging, and a short server
  smoke test on supported Node versions. Do not rely on a pre-existing `dist/`
  directory.

## P2 — Docs and deployment

- [ ] Make README, examples, and implementation agree on transaction atomicity,
  NULL semantics, supported SQL, TTL behavior, and SAVE/LOAD behavior.
- [ ] Document limits, persistence guarantees, AOF recovery rules, cache-only
  durability expectations, invalidation delivery guarantees, and whether
  messages can be lost during reconnect.
- [ ] Harden the Docker image: use a multi-stage build, run as a non-root user,
  avoid the network-dependent `npm install` fallback, add resource/ulimit
  guidance, and make TLS healthchecks verify certificates when configured.
- [ ] Provide a production integration example with explicit cache-key
  versioning, invalidation handling, stale-read policy, and a clear source of
  truth for durable application data.
