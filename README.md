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
- AND: `age > 25 AND name = 'John'`
- OR: `age > 25 OR name = 'John'`
- NOT: `NOT (age > 25)`

## Cache server, counters, batch, pub/sub, persistence (P1)

For multi-instance Echo (Socket.IO scaling), run YASD standalone and share
it via `CACHE_URL=yasd://host:7379?poolSize=4`. One TCP port serves the
RESP-like protocol **and** `GET /healthz` (JSON: entries, hits/misses, …).

```bash
npm run build
node dist/cli.js --port 7379 --snapshot ./data/snapshot.json \
  --aof ./data/appendonly.aof --auto-save-ms 60000
# or: docker compose up --build
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
const stop = await client.subscribe('presence', (ch, msg) => console.log(ch, msg));
await client.publish('presence', JSON.stringify({ user: 'bob' }));

await client.healthcheck(); // { status: 'ok', entries, ... }
await client.save();        // snapshot now (also truncates the AOF)
await stop();
await client.close();
await server.close();       // graceful: drains sockets, final SAVE
```

Protocol commands: `PING GET SET[M PX] CAS MGET MSET DEL CLEAR TTL EXPIRE
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
