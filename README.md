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

#### INSERT
```sql
INSERT INTO table_name (column1, column2) VALUES (value1, value2)
INSERT INTO table_name VALUES (value1, value2)
INSERT INTO table_name VALUES (v1, v2), (v3, v4)
```

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
UPDATE table_name SET col1 = value1, col2 = value2 WHERE condition
```

#### DELETE
```sql
DELETE FROM table_name WHERE condition
```

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
# or: docker compose up --build
```

Protect the RESP port with a password, TLS, or both. Password auth is per
connection; `/livez` and `/readyz` stay open for load balancers. To expose
operational health data, set `YASD_HEALTH_DETAILS=true`. Add
`YASD_HEALTH_TOKEN` to require `Authorization: Bearer <token>` for those
details. Use `yasds://` on clients when TLS is enabled:

Each connection has a bounded pending-output queue of 1 MiB by default. If a
slow subscriber fills its queue, YASD disconnects it. Configure the limit with
`maxPendingOutputBytes`, `--max-pending-output-bytes`, or
`YASD_MAX_PENDING_OUTPUT_BYTES`.

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
await client.save();        // snapshot now (also truncates the AOF)
await stop();
await client.close();
await server.close();       // graceful: drains sockets, final SAVE
```

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
//   aofRecoveryState, aofRecoveryError, subscribers, channels,
//   slowCommandMs, slowLog }
// slowLog: newest-first [{ name, durationMs, at, argc }], capped at 100
```

`aofRecoveryState` is `clean`, `torn-tail`, or `corrupt`. A corrupt AOF stops
replay at the bad line and rejects further AOF writes until it is repaired.

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
