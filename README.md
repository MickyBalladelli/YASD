# YASD - Yet Another Simple Database

A lightweight, SQL-like in-memory database for Node.js. Perfect for testing, prototyping, or small applications that need a simple database interface.

## Features

- **SQL-like syntax** - Familiar CREATE, INSERT, SELECT, UPDATE, DELETE statements
- **In-memory storage** - Fast, no disk I/O
- **Table creation** with typed columns (string, number, boolean)
- **WHERE clauses** with comparison operators (=, !=, >, >=, <, <=, LIKE, IN, BETWEEN)
- **AND/OR/NOT** logical operators
- **ORDER BY** with ASC/DESC sorting
- **LIMIT and OFFSET** for pagination
- **Primary key** support
- **Default values** and NULL handling
- **Column indexes** for faster queries

## Installation

```bash
npm install yasd
```

YASD supports Node.js 18 and newer. `package.json` is the version source of
truth; server `INFO.version` reports that same package version.

## Usage

### Basic Example

```javascript
const { YASD } = require('yasd');

// Create a new database
const db = new YASD();

// Create a table
db.query(`
  CREATE TABLE users (
    id int primary key,
    name string,
    email string,
    age number
  )
`);

// Insert data
db.query("INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', 30)");
db.query("INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com', 25)");

// Query data
const result = db.query('SELECT * FROM users WHERE age > 25');
console.log(result.rows);
// Output: [{ id: 1, name: 'John Doe', email: 'john@example.com', age: 30 }]

// Update data
db.query('UPDATE users SET age = 31 WHERE id = 1');

// Delete data
db.query('DELETE FROM users WHERE id = 2');
```

### Select with Options

```javascript
// Select specific columns
const result = db.query('SELECT name, email FROM users');

// With WHERE clause
const result = db.query('SELECT * FROM users WHERE age > 25 AND name LIKE "%John%"');

// With ORDER BY
const result = db.query('SELECT * FROM users ORDER BY age DESC');

// With LIMIT and OFFSET
const result = db.query('SELECT * FROM users LIMIT 10 OFFSET 5');
```

Each `query()` accepts exactly one statement. A final semicolon is optional;
extra tokens or a second statement are rejected. Multi-row `INSERT` and
multi-row `UPDATE` validate the complete write before changing any rows, so a
failed constraint leaves the statement's earlier rows unchanged.

### Table Management

```javascript
// Get table names
const tables = db.getTableNames();

// Get table schema
const schema = db.getTableSchema('users');

// Drop table
db.query('DROP TABLE users');

// Reset entire database
 db.reset();
```

## API Reference

### `YASD` Class

#### `new YASD()`
Creates a new in-memory database instance.

Pass `indexColumns` to control automatic SQL indexes. By default every column
is indexed; `indexColumns: ['id', 'email']` limits indexes to those names on
each table, and `indexColumns: []` disables automatic indexes.

Use `createDatabase(options)` when a factory function fits better. Every
instance is explicit and should be closed with `db.close()` when finished.

#### `db.query(sql: string): QueryResult`
Executes a SQL query and returns the result.

#### `db.getTableNames(): string[]`
Returns array of all table names in the database.

#### `db.getTableSchema(tableName: string): TableSchema | undefined`
Returns the schema for a specific table, or undefined if not found.

#### `db.reset(): void`
Clears all tables and data from the database.

### QueryResult

```typescript
{
  columns: string[],    // Column names
  rows: Row[],         // Array of result rows
  affectedRows?: number // Number of rows affected by INSERT/UPDATE/DELETE
}
```

### Supported SQL Syntax

#### CREATE TABLE
```sql
CREATE TABLE table_name (
  column1 type [NOT NULL] [DEFAULT value],
  column2 type PRIMARY KEY,
  ...
)
```

Types are `string` (`text`/`varchar`), `number` (`int`/`integer`/`float`/
`decimal`/`numeric`), `boolean` (`bool`), and `any`. A table must contain at
least one column. A primary key may be declared inline or as `PRIMARY KEY
(column)`; it must name an existing column, and there can be only one. Primary
keys are unique and non-NULL.

#### INSERT
```sql
INSERT INTO table_name (column1, column2) VALUES (value1, value2)
INSERT INTO table_name VALUES (value1, value2)
INSERT INTO table_name VALUES (v1, v2), (v3, v4)
```

When a column is omitted, its `DEFAULT` is used; otherwise it receives NULL.
NOT NULL columns and primary keys reject NULL. Row arity, duplicate columns,
type conversion, and primary-key uniqueness are checked before the write.

#### SELECT
```sql
SELECT * FROM table_name
SELECT col1, col2 FROM table_name
SELECT * FROM table_name WHERE condition
SELECT * FROM table_name ORDER BY col1 [ASC|DESC]
SELECT * FROM table_name LIMIT n [OFFSET m]
```

#### UPDATE
```sql
UPDATE table_name SET col1 = value1, col2 = value2 [WHERE condition]
```

#### DELETE
```sql
DELETE FROM table_name [WHERE condition]
```

Joins, aggregates, aliases, and multi-statement scripts are not supported.

#### DROP TABLE
```sql
DROP TABLE table_name
```

### WHERE Conditions

- Comparison: `=`, `!=`, `>`, `>=`, `<`, `<=`
- LIKE: `name LIKE '%John%'` (supports `%` and `_` wildcards)
- IN: `id IN (1, 2, 3)`
- BETWEEN: `age BETWEEN 20 AND 40`
- NULL checks: `column IS NULL` and `column IS NOT NULL`
- AND: `age > 25 AND name = 'John'`
- OR: `age > 25 OR name = 'John'`
- NOT: `NOT (age > 25)`

ORDER BY places NULL values last for both ASC and DESC. When an `any` column
contains mixed types, non-NULL values sort by type (`number`, `string`,
`boolean`, `array`, then `object`), then by value. Equal sort keys keep their
input row order.

String literals use single or double quotes. Escape characters with backslash:
`\\`, `\'`, `\"`, `\n`, `\r`, `\t`, `\0`, `\b`, `\f`, and `\v`; SQL-style
doubled delimiters (`''` and `""`) are also supported. `NULL`, `TRUE`, and
`FALSE` are case-insensitive.

NULL follows SQL three-valued logic. Ordinary comparisons involving NULL are
UNKNOWN and do not match a `WHERE` clause; use `IS NULL` or `IS NOT NULL` for
explicit NULL checks. AND, OR, and NOT preserve UNKNOWN through compound
predicates.

## Cache server, counters, batch, pub/sub, persistence (P1)

For multi-instance Echo (Socket.IO scaling), run YASD standalone and share
it via `CACHE_URL=yasd://host:7379?poolSize=4`. One TCP port serves the
RESP-like protocol plus `GET /livez`, `GET /readyz`, and `GET /healthz`.
`/livez` is minimal liveness, `/readyz` is readiness, and `/healthz` is
redacted by default (`{"status":"ok"}`).

```bash
npm run build
node dist/cli.js --port 7379 --snapshot ./data/snapshot.json \
  --aof ./data/appendonly.aof --auto-save-ms 60000
# or: docker compose up --build  # builds src inside a multi-stage image
```

### Docker deployment

The Dockerfile builds from `src/` with `npm ci` and fails if the lockfile is
not usable; it does not copy or require a pre-existing `dist/` directory and
does not fall back to an unconstrained install. The runtime image contains
only the compiled server and package metadata, runs as the unprivileged
`yasd` user, and writes persistence files only under `/data`.

The Compose example makes the root filesystem read-only, drops Linux
capabilities, enables `no-new-privileges`, and sets a 512 MiB memory cap, one
CPU, 256 processes, and a 65,536-file descriptor limit. Keep the memory cap
comfortably above `CACHE_MAX_BYTES` for V8, sockets, temporary snapshots, and
the AOF. If you use a bind mount instead of the named `yasd-data` volume, make
sure the host directory is writable by the container's `yasd` user (UID/GID
created by the image).

The container healthcheck calls `/readyz`. Without TLS it uses HTTP. When
`YASD_TLS_KEY` or `YASD_TLS_CERT` is set, it uses HTTPS with certificate and
hostname verification enabled. Public-CA certificates need no extra setting;
for a private CA, mount the CA and set `YASD_HEALTHCHECK_CA`. Set
`YASD_HEALTHCHECK_SERVERNAME` when the certificate name differs from
`127.0.0.1`. If the server requires client certificates, also set matching
`YASD_HEALTHCHECK_CLIENT_CERT` and `YASD_HEALTHCHECK_CLIENT_KEY`. A missing or
invalid trust/client-certificate setup makes the healthcheck fail instead of
silently accepting an unverified TLS connection.

Protect the RESP port with a password, TLS, or both. Password auth is per
connection; `/livez` and `/readyz` stay open for load balancers. To expose
operational health data, set `YASD_HEALTH_DETAILS=true`. Add
`YASD_HEALTH_TOKEN` to require `Authorization: Bearer <token>` for those
details. Use `yasds://` on clients when TLS is enabled:

Each connection has a bounded pending-output queue of 1 MiB by default. If a
slow subscriber fills its queue, YASD disconnects it. Configure the limit with
`maxPendingOutputBytes`, `--max-pending-output-bytes`, or
`YASD_MAX_PENDING_OUTPUT_BYTES`.

Cache writes also have separate UTF-8 limits: `maxKeyBytes` defaults to 1 KiB
and `maxValueBytes` defaults to 4 MiB. Set them under `cache`, or with
`--max-key-bytes` / `--max-value-bytes` and `CACHE_MAX_KEY_BYTES` /
`CACHE_MAX_VALUE_BYTES`. The aggregate `maxBytes` limit still applies.

Operational limits are opt-in: `maxCommandMs` returns an error and closes a
connection after an over-budget synchronous command, `idleConnectionTimeoutMs`
closes inactive connections, and `shutdownDeadlineMs` bounds socket draining
and the final persistence wait. Their CLI/env forms are
`--max-command-ms` / `YASD_MAX_COMMAND_MS`,
`--idle-connection-timeout-ms` / `YASD_IDLE_CONNECTION_TIMEOUT_MS`, and
`--shutdown-deadline-ms` / `YASD_SHUTDOWN_DEADLINE_MS`. A value of `0`
disables the first two; shutdown defaults to 2 seconds. JavaScript cannot
interrupt a synchronous command already running, so the command limit acts
when that command returns.

```bash
YASD_PASSWORD='change-me' \
YASD_TLS_KEY=./tls/server.key \
YASD_TLS_CERT=./tls/server.crt \
node dist/cli.js --port 7379
```

The same settings use `--password`, `--tls-key`, `--tls-cert`, and `--tls-ca`
flags. `YASD_TLS_CA` is optional; combine it with
`YASD_TLS_REQUEST_CERT=true` and `YASD_TLS_REJECT_UNAUTHORIZED=true` for
client-certificate verification. `YASD_TLS_MIN_VERSION` accepts `TLSv1.2` or
`TLSv1.3` (older protocol versions are accepted only when explicitly chosen).

```javascript
const secureClient = new YasdClient({
  url: 'yasds://:change-me@127.0.0.1:7379?poolSize=4',
  tls: { ca: require('node:fs').readFileSync('./tls/ca.crt') }
})
await secureClient.connect()
await secureClient.healthcheck() // HTTPS healthcheck when TLS is enabled
```

```javascript
const { YasdServer, YasdClient } = require('yasd');

// Embedded or standalone server
const server = new YasdServer({ port: 7379, snapshotPath: './data/snapshot.json' });
await server.start();

// Pooled client (pipelining-safe). Values round-trip as JSON.
const client = YasdClient.fromEnv(); // CACHE_URL
await client.connect();
await client.set('feeds:home', { posts: [] }, 15_000);
await client.mset([{ key: 'a', value: 1 }, { key: 'b', value: [2] }]);
await client.mget(['a', 'b']); // [1, [2]]

// Atomic counters (rate limits, unread/like counts). TTL is preserved.
await client.incr('ratelimit:post:alice');
await client.expire('ratelimit:post:alice', 60_000);

// Pub/sub (invalidation, presence/typing). The server also publishes every
// mutation on `__yasd__:invalidate` for other replicas to consume.
// Invalidation events: set, del, clear, expire, persist, and load.
const stop = await client.subscribe('presence', (ch, msg) => console.log(ch, msg));
await client.publish('presence', JSON.stringify({ user: 'bob' }));
await client.reconnectSubscriptions() // restore registered channels after a drop

await client.healthcheck(); // { status: 'ok' } unless health details are enabled
await client.info();         // includes safe persistence status and limits
await client.save();        // snapshot now (rotates the AOF)
await stop();
await client.close();
await server.close();       // graceful: drains sockets, final SAVE when configured
```

### TTL and persistence behavior

`set(key, value, ttlMs)` uses the explicit TTL when supplied. Without one,
known namespaces use their configured default and other keys persist. `ttl()`
returns remaining milliseconds, `-1` for a persistent key, or `-2` for a
missing/expired key. `expire()` replaces a TTL, `persist()` removes it, and
`ttlMs: 0` expires the key immediately. Expiry is lazy on access and can also
be removed by the background sweeper.

Snapshots contain live entries and their absolute expiry deadlines. When
`loadOnStart` is enabled, a server startup loads the snapshot first, then
replays newer AOF records, so a restart does not extend a key's TTL. `SAVE`
writes an atomic snapshot and rotates the AOF while retaining records written
after that snapshot. `LOAD` replaces the cache with the selected snapshot; it
does not replay the AOF and publishes a cache invalidation event. `SAVE` does
not publish an invalidation because it does not change cache contents.

### Limits, durability, recovery, and delivery guarantees

The default resource limits are:

| Resource | Default | Behavior |
| --- | ---: | --- |
| Live cache entries | 10,000 | Oldest LRU entries are evicted first |
| Aggregate cache bytes | 64 MiB | UTF-8 key bytes plus UTF-8 JSON value bytes |
| One key | 1 KiB | Larger keys are rejected |
| One JSON value | 4 MiB | Larger values are rejected |
| One RESP frame | 8 MiB | The connection is closed on a limit violation |
| RESP bulk payload | 4 MiB | The connection is closed on a limit violation |
| RESP array items | 1,024 | Applies to command arguments and nested arrays |
| RESP nesting depth | 32 | Deeper arrays are rejected |
| Buffered RESP input | 8 MiB | Excess incomplete input is rejected |
| Pending socket output | 1 MiB | Slow consumers are disconnected |
| HTTP request headers | 16 KiB | Larger or malformed headers are rejected |

Cache limits are configurable through `YasdServer({ cache: ... })` and the
documented CLI/environment settings. A value that exceeds either its own
limit or `maxBytes` is rejected; it is never stored temporarily and never
evicts the entry that caused the oversized write. `maxBytes` is not a heap
size limit: it counts only each key and its JSON representation.

`maxCommandMs`, `idleConnectionTimeoutMs`, and `shutdownDeadlineMs` are
additional operational controls. The first two are disabled by default;
`shutdownDeadlineMs` defaults to 2 seconds. Client requests time out after 5
seconds by default, and the command pool defaults to four connections (maximum
1,024). There is no separate SQL statement-size limit today; process memory
remains the boundary for embedded SQL parsing.

Durability is optional and cache data is not a source of truth:

- With no `snapshotPath` and no `aofPath`, all data is process memory. A
  restart, crash, `SIGKILL`, or host loss can remove everything.
- A snapshot is written to a temporary file, synced, atomically renamed, and
  directory-synced. It contains live values and absolute expiry deadlines.
  Without an AOF, writes since the last completed snapshot can be lost.
- An AOF records accepted cache mutations as JSON lines. A normal append is
  synchronous, but does not call `fsync` for every write; an OS or power crash
  can lose recent accepted records. A failed AOF append returns an error and
  rolls back that cache mutation.
- With both files configured, startup restores the snapshot first and applies
  only newer sequenced AOF records. `SAVE` records the covered AOF sequence and
  rotates the log while retaining mutations that arrived during the save.
  This makes snapshot/AOF crash windows recoverable without replaying covered
  mutations twice. `saveOnShutdown` only runs when a snapshot path is
  configured, and its final wait is bounded by `shutdownDeadlineMs`.
- `loadOnStart` defaults to true. `LOAD` is a runtime replacement: it loads
  only the selected snapshot, does not replay or rotate the AOF, and is not a
  restart checkpoint by itself. Run `SAVE` after `LOAD` when the loaded state
  must become the restart baseline.

AOF recovery is deliberately conservative. Startup replays valid records in
order and stops at the first middle corruption, invalid record, or sequence
error; records after that line are ignored. The server exposes
`aofRecoveryState` as `clean`, `torn-tail`, or `corrupt`. An incomplete final
JSON line is treated as a torn tail and discarded automatically. A corrupt
AOF leaves readiness failing and rejects further AOF-backed writes until an
operator repairs or replaces the file and restarts the server. Inspect
`INFO.persistence` or embedded `server.persistenceStatus()` for safe error
codes; raw paths and filesystem messages are not exposed.

`__yasd__:invalidate` is a cache-invalidation hint channel, not a durable
log. It publishes `set`, `del`, `clear`, `expire`, `persist`, and `load` after
the corresponding cache commit (and after AOF append when AOF is enabled).
Delivery is synchronous and ordered for listeners in one process, but network
delivery is best-effort: there are no message IDs, acknowledgements, replay,
or cross-server delivery guarantee. LRU evictions are local capacity events
and do not publish invalidations. Expiry invalidation happens when lazy access
or the sweeper notices expiry; disabling the sweeper can delay that hint.
Consumers must tolerate missed hints and re-read the durable application
source of truth when correctness matters.

Command connections reconnect lazily when a later command needs them. A dead
connection rejects its pending requests; the client does not blindly retry a
command because retrying a write could duplicate it. Reconnection uses at
most four dials with bounded exponential backoff. Subscriber connections need
`reconnectSubscriptions()` (or a later `subscribe()` call) to restore their
registered channels. Messages published while a subscriber is disconnected,
while its subscription is being restored, or after a slow-consumer disconnect
are lost. Resubscription restores the channel, not the missed history.

Protocol commands: `AUTH PING GET SET[M PX] CAS MGET MSET DEL CLEAR TTL EXPIRE
PERSIST INCR[BY] DECR[BY] WATCH UNWATCH MULTI EXEC DISCARD
PUBLISH SUBSCRIBE UNSUBSCRIBE INFO SAVE LOAD QUIT`.

### Transactions (multi-key read-modify-write)

Single-key RMW is covered by `CAS`/`INCR`/`DECR`. For multi-key atomicity
(feed + counter updates, single-flight hydration), use optimistic
transactions — embedded or over the wire with identical semantics:

```javascript
// Embedded
const tx = db.multi();
tx.watch('likes:1', 'feed:home');
const likes = tx.get('likes:1') ?? 0;
tx.set('likes:1', likes + 1);
tx.set('feed:home', freshFeed, 15_000);
const results = tx.exec(); // null = watched key changed, retry
if (results === null) { /* re-read and retry */ }

// Or with automatic retries:
const out = await db.runTransaction(['likes:1', 'feed:home'], async (tx) => {
  const likes = tx.get('likes:1') ?? 0;
  tx.set('likes:1', likes + 1);
  tx.set('feed:home', freshFeed, 15_000);
  return likes + 1;
}, 3 /* maxRetries */);
// out = { committed, attempts, results, value }
```

```javascript
// Server mode (dedicated connection, same pattern)
const tx = client.multi();
await tx.watch('likes:1');
const cur = await tx.get('likes:1');
await tx.set('likes:1', (cur ?? 0) + 1); // first write auto-sends MULTI
const results = await tx.exec(); // null on conflict; per-op array otherwise
// Or: await client.runTransaction(['likes:1'], async (tx) => { ... });
```

Rules: `WATCH` snapshots key versions; `EXEC` commits the queued writes
atomically and returns per-op replies, or nil when a watched key changed
(nothing applied). `EXEC`/`DISCARD` always clear watches (`DISCARD` drops the
queue too). Committed writes hit the AOF and fan out invalidations exactly
like plain writes. Reads must precede `MULTI` — read first, then write.

### Metrics: counters, memory, slow-query log

```javascript
// Embedded KV counters (cumulative)
db.cacheStats();
// { hits, misses, expiries, evictions, entries, bytes }
db.resetStats(); // zero the four counters; entries/bytes untouched

// Embedded SQL slow-query log (0 = off)
const db2 = new YASD({ slowQueryMs: 5 });
db2.setSlowQueryThreshold(5);
db2.slowLog();      // newest-first [{ sql, durationMs, at }], capped at 100
db2.clearSlowLog();
```

Server mode exposes the same counters plus a slow-**command** log through
`INFO`. `/healthz` serves that operational JSON only when health details are
enabled; otherwise it stays redacted.

```bash
node dist/cli.js --slow-command-ms 5   # or YASD_SLOW_COMMAND_MS=5
```

```javascript
await client.info();
// { status, version, uptimeMs, connections, tls, auth,
//   entries, bytes, hits, misses, evictions, expiries,
//   aofRecoveryState, aofRecoveryError, persistence, subscribers, channels,
//   slowCommandMs, slowLog }
// slowLog: newest-first [{ name, durationMs, at, argc }], capped at 100
```

`aofRecoveryState` is `clean`, `torn-tail`, or `corrupt`. A corrupt AOF stops
replay at the bad line and rejects further AOF writes until it is repaired.
`persistence.errors` contains only safe component/operation/error-code/time
metadata; it never includes file paths, cache keys, values, or raw filesystem
messages. Use `server.persistenceStatus()` in embedded mode or `INFO` over the
wire to inspect it.

The equivalent server options are `health: { exposeDetails: true, token }`.
When `token` is set, details require `Authorization: Bearer <token>`.

Conventions: `hits`/`misses` count cache lookups (`get`/`mget`, plus `ttl`
misses); `bytes` tracks key + JSON value size; slow thresholds are in ms
(fractions allowed — `1e-9` logs everything, handy for tests).

### Query profiling: EXPLAIN and PROFILE

```javascript
db.explain("SELECT * FROM users WHERE id = 7 ORDER BY age DESC LIMIT 10");
// { statement: 'select', table: 'users', columns: '*',
//   strategy: 'index-scan', indexColumns: ['id'],
//   hasOrderBy: true, orderBy: { column: 'age', direction: 'desc' },
//   limit: 10, tableRows: 200 }

db.profile('SELECT * FROM users WHERE age > 40');
// { ...plan, durationMs: 0.42, rowsReturned: 54 }
// writes also report affectedRows
```

`explain()` plans without running: `=` / `IN` (including `AND`s of those) on
indexed columns report `index-scan` with the columns used; other predicates,
`LIKE`, `OR`, and mixed predicates report `full-scan`. UPDATE and DELETE use
the same equality/IN index plans internally. Non-`SELECT` statements report
strategy `'n/a'`. `profile()` runs the query and adds sub-ms `durationMs`,
`rowsReturned`, and `affectedRows` for writes — and the run feeds the
slow-query log when over threshold.

Run `npm run benchmark:indexes -- 10000 1000` to compare indexed, allowlisted,
and index-free memory, insert, update, and equality-query costs. It prints one
JSON result per configuration.

Run `npm run benchmark:load -- 5000 4 5000` for concurrent load coverage of
LRU, expiry sweeping, indexed and full-scan reads, UPDATE/DELETE, pub/sub
fanout, pooled client traffic, AOF appends, and snapshot saves. Results include
p50/p95/p99/max latency, throughput, heap/RSS deltas and peaks. The persistence
run leaves artifacts under a temporary directory named in its JSON output.

CI runs on Node 18, 20, 22, and 24. Each job starts from a checkout without
`dist/`, runs the build, type check, tests, package dry-run, and a short
server/client smoke test.

## Test Server

A test server is included in the `test/` directory. Run it to see YASD in action:

```bash
# First, build the package
npm run build

# Then run the test server
node test/server.js
```

Then open your browser to `http://localhost:3000` to interact with the database through a web interface.

## Development

```bash
# Install dependencies
npm install

# Build the package
npm run build

# Run tests
npm test
```

## License

MIT
