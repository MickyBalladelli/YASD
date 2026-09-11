// Reproductions from TODO2.md. Run serially: controlled clocks and disk fault injection are process-local.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { once } = require('node:events');
const {
  YASD, KVCache, YasdServer, YasdClient, YasdTransaction, AofLog,
  saveSnapshot, loadSnapshot, DatabaseError, encodeCommand, RespDecoder,
} = require('../dist');
const { resolveCliOptions } = require('../dist/cli');
const { isIncompleteJson } = require('../dist/json-prefix');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'yasd-review-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return name => path.join(dir, name);
}
async function running(t, options = {}) {
  const server = new YasdServer({ host: '127.0.0.1', port: 0, saveOnShutdown: false, ...options });
  await server.start();
  const client = new YasdClient({ host: '127.0.0.1', port: server.address().port, poolSize: 1 });
  t.after(async () => { await client.close(); await server.close(); });
  return { server, client };
}
async function wire(t, port) {
  const socket = net.createConnection({ host: '127.0.0.1', port });
  t.after(() => socket.destroy());
  const replies = [];
  const decoder = new RespDecoder();
  socket.on('data', chunk => replies.push(...decoder.push(chunk)));
  socket.on('error', () => {});
  await once(socket, 'connect');
  return { socket, replies, async read(n) {
    for (let i = 0; replies.length < n && i < 1000; i++) await pause(2);
    assert.ok(replies.length >= n, `expected ${n} replies; got ${JSON.stringify(replies)}`);
    return replies.splice(0, n);
  }};
}

test('SQL: assignment commas, parentheses, BETWEEN and strict statement grammar', () => {
  const db = new YASD({ sweepIntervalMs: 0 });
  try {
    db.query('CREATE TABLE t (id int primary key, a string, b number)');
    db.query("INSERT INTO t VALUES (5, 'old', 1), (15, 'other', 2)");
    db.query("UPDATE t SET a = 'new', b = 3 WHERE NOT (id > 5)");
    assert.deepEqual(db.query('SELECT a, b FROM t WHERE id = 5').rows, [{ a: 'new', b: 3 }]);
    assert.deepEqual(db.query('SELECT id FROM t WHERE NOT (id BETWEEN 10 AND NULL)').rows, [{ id: 5 }]);
    for (const sql of ['SELECT * FROM t;;', 'SELECT * FROM t LIMIT -1',
      'INSERT INTO t VALUES (1 2 3)', 'INSERT INTO t (id a) VALUES (1, 2)',
      'CREATE TABLE bad (id varchar(oops))', 'CREATE TABLE bad (id numeric(2,3))',
      'CREATE TABLE bad (id int,)', 'INSERT INTO t VALUES (2, 3,)']) {
      assert.throws(() => db.query(sql), error => error instanceof DatabaseError && error.code === 'PARSE_ERROR', sql);
    }
  } finally { db.close(); }
});

test('SQL: owned schemas and special property names', () => {
  const db = new YASD({ sweepIntervalMs: 0 });
  try {
    db.query('CREATE TABLE special (id int primary key, __proto__ string, constructor string)');
    db.query("INSERT INTO special VALUES (1, 'safe', 'ctor')");
    const schema = db.getTableSchema('special');
    schema.primaryKey = undefined; schema.columns[0].type = 'string'; schema.columns.length = 0;
    assert.throws(() => db.query("INSERT INTO special VALUES (1, 'bad', 'bad')"), error => error.code === 'PRIMARY_KEY_CONSTRAINT');
    assert.deepEqual(db.query('SELECT * FROM special').rows[0], JSON.parse('{"id":1,"__proto__":"safe","constructor":"ctor"}'));
  } finally { db.close(); }
});

test('JSON contract: embedded and wire reject normalization, getters and hooks', async t => {
  const { client } = await running(t);
  const cache = new KVCache({ sweepIntervalMs: 0 }); t.after(() => cache.close());
  let calls = 0;
  const getter = Object.defineProperty({}, 'value', { enumerable: true, get() { calls++; return 1; } });
  const custom = Object.defineProperty({}, 'toJSON', { value() { calls++; return 1; } });
  const array = [1]; array.toJSON = () => 2;
  for (const value of [NaN, Infinity, { a: undefined }, new Date(), [, 1], getter, custom, array]) {
    assert.throws(() => cache.set('bad', value), error => error.code === 'INVALID_VALUE');
    await assert.rejects(client.set('bad', value), error => error.code === 'INVALID_VALUE');
  }
  assert.equal(calls, 0);
  const value = JSON.parse('{"__proto__":{"safe":true},"nested":[1]}');
  cache.set('owned', value); await client.set('owned', value);
  value.nested.push(2);
  assert.deepEqual(cache.get('owned'), await client.get('owned'));
  assert.equal(Object.getPrototypeOf(cache.get('owned')), Object.prototype);
});

test('CAS retains persistent namespace keys; WATCH catches expiry and tombstone ABA', () => {
  const original = Date.now; let now = original(); Date.now = () => now;
  const db = new YASD({ maxEntries: 1, sweepIntervalMs: 0 });
  try {
    db.set('feeds:x', 1); db.persist('feeds:x'); assert.equal(db.cas('feeds:x', 1, 2), true);
    assert.equal(db.ttl('feeds:x'), -1);
    db.set('watched', 1, 10);
    const expired = db.multi(); expired.watch('watched'); expired.set('out', 1);
    now += 20; assert.equal(expired.exec(), null);
    db.set('watched', 1); const aba = db.multi(); aba.watch('watched');
    db.del('watched');
    for (let i = 0; i < 2200; i++) { db.set(`churn:${i}`, i); db.del(`churn:${i}`); }
    db.set('watched', 1); aba.set('out', 2); assert.equal(aba.exec(), null);
  } finally { Date.now = original; db.close(); }
});

test('snapshot replacement rejects bad values, deadlines and duplicate keys atomically', async t => {
  const file = fixture(t)('snapshot.json');
  const cache = new KVCache({ sweepIntervalMs: 0, maxValueBytes: 20 }); t.after(() => cache.close());
  cache.set('preserved', 1, 60_000); const before = cache.dump(); const stats = cache.stats();
  const version = cache.getVersion('preserved');
  for (const entries of [ [{ key: 'bad', value: 'x'.repeat(30) }],
    [{ key: 'bad', value: 1, expiresAt: 'never' }], [{ key: 'dup', value: 1 }, { key: 'dup', value: 2 }],
    [{ value: 1 }] ]) {
    fs.writeFileSync(file, JSON.stringify({ version: 2, savedAt: Date.now(), entries }));
    await assert.rejects(loadSnapshot(cache, file));
    assert.deepEqual(cache.dump(), before); assert.deepEqual(cache.stats(), stats);
    assert.equal(cache.getVersion('preserved'), version);
  }
});

test('AOF: final state retains PERSIST/extension and never resurrects expired INCR', async t => {
  const aofPath = fixture(t)('state.aof');
  const original = Date.now; let now = original(); Date.now = () => now;
  const server = new YasdServer({ aofPath, cache: { sweepIntervalMs: 0 }, saveOnShutdown: false });
  try {
    server.cache.set('expired', 10, 100); server.cache.incr('expired');
    server.cache.set('persistent', 7, 100); server.cache.persist('persistent');
    server.cache.set('extended', 8, 100); server.cache.expire('extended', 1000);
    now += 200;
    const cache = new KVCache({ sweepIntervalMs: 0 });
    try {
      const log = new AofLog(aofPath); await log.replay(cache);
      assert.equal(log.recoveryState, 'clean');
      assert.equal(cache.get('expired'), undefined);
      assert.equal(cache.get('persistent'), 7); assert.equal(cache.ttl('persistent'), -1);
      assert.equal(cache.get('extended'), 8); assert.equal(cache.ttl('extended'), 800);
    } finally { cache.close(); }
  } finally { Date.now = original; await server.close(); }
});

test('AOF: eviction tombstones preserve membership despite unlogged reads', async t => {
  const aofPath = fixture(t)('eviction.aof');
  const server = new YasdServer({ aofPath, cache: { maxEntries: 2, sweepIntervalMs: 0 } });
  t.after(() => server.close());
  server.cache.set('a', 1); server.cache.set('b', 2); server.cache.get('a'); server.cache.set('c', 3);
  const cache = new KVCache({ maxEntries: 2, sweepIntervalMs: 0 }); t.after(() => cache.close());
  await new AofLog(aofPath).replay(cache);
  assert.deepEqual(cache.dump().map(e => e.key).sort(), ['a', 'c']);
});

test('alternate SAVE is an export; missing AOF still advances snapshot sequence', async t => {
  const file = fixture(t); const snapshotPath = file('main.json'); const aofPath = file('main.aof');
  let server = new YasdServer({ snapshotPath, aofPath, port: 0, saveOnShutdown: false });
  t.after(async () => { await server.close(); });
  await server.start(); server.cache.set('old', 1); await server.save();
  server.cache.set('new', 2); await server.save(file('export.json')); await server.close();
  server = new YasdServer({ snapshotPath, aofPath, port: 0, saveOnShutdown: false }); await server.start();
  assert.equal(server.cache.get('new'), 2); await server.save(); await server.close();
  fs.unlinkSync(aofPath);
  server = new YasdServer({ snapshotPath, aofPath, port: 0, saveOnShutdown: false }); await server.start();
  server.cache.set('after-missing', 3); await server.close();
  server = new YasdServer({ snapshotPath, aofPath, port: 0, saveOnShutdown: false }); await server.start();
  assert.equal(server.cache.get('after-missing'), 3);
});

test('AOF invalid MSET rolls back its entire record', async t => {
  const file = fixture(t)('bad.aof');
  fs.writeFileSync(file, JSON.stringify({ version: 1, seq: 1, op: { op: 'mset', entries: [
    { key: 'first', value: 1 }, { key: 'invalid', value: 'x'.repeat(50) },
  ] } }) + '\n');
  const cache = new KVCache({ maxValueBytes: 20, sweepIntervalMs: 0 }); t.after(() => cache.close());
  cache.set('before', 7);
  const log = new AofLog(file);
  assert.equal(await log.replay(cache), 0); assert.equal(log.recoveryState, 'corrupt');
  assert.equal(cache.get('first'), undefined); assert.equal(cache.get('before'), 7);
});

test('AOF truncation scanner accepts only valid incomplete JSON prefixes', () => {
  const record = JSON.stringify({ version: 1, seq: 1, op: { op: 'set', key: 'quoted"', value: [null, true, -1.5e23] } });
  for (let i = 1; i < record.length; i++) assert.equal(isIncompleteJson(record.slice(0, i)), true, `cut ${i}`);
  for (const corrupt of ['not json', '{"x":nope', '{"x":1.2.', '{"x":1e2e', '{"x":1,}', '{]']) {
    assert.equal(isIncompleteJson(corrupt), false, corrupt);
  }
});

test('AOF partial append fails closed and memory rolls back', async t => {
  const file = fixture(t)('partial.aof');
  const server = new YasdServer({ aofPath: file, cache: { sweepIntervalMs: 0 } }); t.after(() => server.close());
  server.cache.set('old', 1);
  const append = fs.appendFileSync;
  fs.appendFileSync = (target, value, encoding) => {
    append(target, String(value).slice(0, 20), encoding);
    const error = new Error('injected disk full'); error.code = 'ENOSPC'; throw error;
  };
  try { assert.throws(() => server.cache.set('new', 2), /AOF write failed/); }
  finally { fs.appendFileSync = append; }
  assert.equal(server.cache.get('new'), undefined);
  assert.throws(() => server.cache.set('later', 3), /AOF write failed/);
  assert.equal(server.cache.get('later'), undefined);
  const log = new AofLog(file); const cache = new KVCache({ sweepIntervalMs: 0 }); t.after(() => cache.close());
  await log.replay(cache); assert.equal(cache.get('old'), 1); assert.equal(log.recoveryState, 'torn-tail');
  log.append({ op: 'set', key: 'repaired', value: 4 });
  assert.equal(log.recoveryState, 'clean');
});

test('wire FIFO: SAVE, write, LOAD, GET, PING and QUIT remain ordered', async t => {
  const { server } = await running(t, { snapshotPath: fixture(t)('fifo.json') });
  server.cache.set('value', 1);
  const raw = await wire(t, server.address().port);
  raw.socket.write(Buffer.concat([
    ['SAVE'], ['SET', 'value', '2'], ['LOAD'], ['GET', 'value'], ['PING'], ['QUIT'],
  ].map(encodeCommand)));
  const replies = await raw.read(6);
  assert.deepEqual(replies.map(r => r.value), ['OK', 'OK', 'OK 1', '1', 'PONG', 'OK']);
});

test('RESP protocol choice survives every alphabetic payload split', async t => {
  const { server } = await running(t);
  const raw = await wire(t, server.address().port);
  const frame = encodeCommand(['PING', 'alphabetic payload']);
  for (let i = 1; i < frame.length; i++) {
    raw.socket.write(frame.subarray(0, i)); await pause(1); raw.socket.write(frame.subarray(i));
    assert.equal((await raw.read(1))[0].value, 'alphabetic payload');
  }
});

test('HTTP rejects a complete oversized header', async t => {
  const { server } = await running(t);
  const socket = net.createConnection({ host: '127.0.0.1', port: server.address().port }); t.after(() => socket.destroy());
  let text = ''; socket.on('data', b => { text += b; });
  await once(socket, 'connect');
  const end = once(socket, 'end');
  socket.write(`GET /healthz HTTP/1.1\r\nX-Large: ${'x'.repeat(17_000)}\r\n\r\n`);
  await end; assert.match(text, /^HTTP\/1.1 431/);
});

test('client transactions: empty exec, fresh reads, concurrent first writes and parent close', async t => {
  const { client } = await running(t);
  assert.deepEqual(await client.multi().exec(), []);
  await client.set('read', 1);
  const tx = client.multi(); assert.equal(await tx.get('read'), 1); assert.equal(await tx.ttl('read'), -1);
  assert.deepEqual(await tx.mget(['read']), [1]);
  await Promise.all([tx.set('a', 1), tx.set('b', 2)]);
  assert.deepEqual(await tx.exec(), ['OK', 'OK']);
  const abandoned = client.multi(); await abandoned.watch('read');
  await client.close(); assert.equal(abandoned.finished, true);
});

test('transaction queue limits abort without applying the accepted prefix', async t => {
  const { server } = await running(t, { maxTransactionCommands: 1, maxWatchedKeys: 1 });
  const raw = await wire(t, server.address().port);
  raw.socket.write(Buffer.concat([['MULTI'], ['SET', 'first', '1'], ['SET', 'second', '2'], ['EXEC'], ['PING']].map(encodeCommand)));
  const replies = await raw.read(5);
  assert.equal(replies[1].value, 'QUEUED'); assert.equal(replies[2].kind, 'error');
  assert.equal(replies[3].kind, 'error'); assert.equal(replies[4].value, 'PONG');
  assert.equal(server.cache.get('first'), undefined);
  raw.socket.write(Buffer.concat([['WATCH', 'a'], ['WATCH', 'b']].map(encodeCommand)));
  assert.equal((await raw.read(2))[1].kind, 'error');
});

test('large finite counter results and fractional TTLs remain decodable', async t => {
  const { client } = await running(t);
  await client.set('large', 1e20); assert.equal(await client.incr('large', 1e20), 2e20);
  await client.set('fraction', 1, 10000.5); assert.ok(Number.isSafeInteger(await client.ttl('fraction')));
  assert.throws(() => require('../dist/protocol').encodeInt(Number.MAX_SAFE_INTEGER + 1), /safe integers/);
});

test('configuration: negative flags, false env, precedence and secret redaction', () => {
  const options = resolveCliOptions(['--no-load', '--no-save-on-shutdown', '--port', '0', '--max-watched-keys', '7'],
    { YASD_LOAD_ON_START: 'true', YASD_PORT: 'bad' });
  assert.equal(options.loadOnStart, false); assert.equal(options.saveOnShutdown, false);
  assert.equal(options.port, 0); assert.equal(options.maxWatchedKeys, 7);
  assert.equal(resolveCliOptions([], { YASD_LOAD_ON_START: 'false' }).loadOnStart, false);
  assert.throws(() => resolveCliOptions(['--unknown'], {}), /unknown/);
  assert.throws(() => new YasdServer({ autoSaveMs: 2 ** 32 }), /autoSaveMs/);
  assert.throws(() => new YasdServer({ loadOnStart: 'false' }), /true or false/);
  assert.throws(() => new YasdServer({ snapshotPath: 'same', aofPath: './same' }), /different/);
  assert.throws(() => new YasdClient({ url: 'yasd://:private%ZZ@localhost' }), error => !error.message.includes('private'));
  assert.throws(() => new YasdClient({ tls: { host: 'other' } }), /routing/);
});

test('lifecycle: joined start/close and truthful shutdown failure', async t => {
  const server = new YasdServer({ port: 0 });
  const start = server.start(); assert.equal(server.start(), start); await start;
  const close = server.close(); assert.equal(server.close(), close); await close;
  await assert.rejects(server.start(), error => error.code === 'CONNECTION_CLOSED');
  const invalid = fixture(t)('directory'); fs.mkdirSync(invalid);
  const broken = new YasdServer({ snapshotPath: invalid, saveOnShutdown: true, loadOnStart: false, port: 0 });
  await broken.start();
  await assert.rejects(broken.close(), error => error.code === 'PERSISTENCE_ERROR');
});

test('TCP/TLS handshake deadline and close cancel pending dials', async t => {
  const sockets = new Set();
  const blackhole = net.createServer(socket => { sockets.add(socket); socket.on('error', () => {}); });
  await new Promise(resolve => blackhole.listen(0, '127.0.0.1', resolve));
  t.after(async () => { for (const socket of sockets) socket.destroy(); await new Promise(resolve => blackhole.close(resolve)); });
  const tx = new YasdTransaction({ host: '127.0.0.1', port: blackhole.address().port,
    tlsOptions: { rejectUnauthorized: false }, connectTimeoutMs: 30 });
  await assert.rejects(tx.connect(), error => error.code === 'TIMEOUT'); await tx.close();
  const cancelled = new YasdTransaction({ host: '127.0.0.1', port: blackhole.address().port,
    tlsOptions: { rejectUnauthorized: false }, connectTimeoutMs: 3000 });
  const attempt = cancelled.connect(); const assertion = assert.rejects(attempt);
  await pause(5); await cancelled.close(); await assertion;
});
