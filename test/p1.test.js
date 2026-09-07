#!/usr/bin/env node
// P1 regression tests: counters, batch ops, pub/sub, persistence,
// protocol codec, and server+client end to end.
// Run: node test/p1.test.js (needs the built dist + networking on localhost).

let mod;
try {
  mod = require('../dist/index.js');
  console.log('Using compiled version from dist/index.js');
} catch (e) {
  console.log('YASD not available, skipping tests');
  console.log('Please run: npm run build');
  process.exit(0);
}

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');

const { YASD, YasdServer, YasdClient, parseCacheUrl, saveSnapshot, loadSnapshot, AofLog } = mod;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function tmpDir() {
  const dir = path.join(os.tmpdir(), `yasd-p1-${process.pid}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function runTests() {
  console.log('Starting YASD P1 tests...\n');

  let passed = 0;
  let failed = 0;

  async function test(name, fn) {
    try {
      await fn();
      console.log(`✓ ${name}`);
      passed++;
    } catch (error) {
      console.log(`✗ ${name}`);
      console.log(`  Error: ${error && error.message}`);
      failed++;
    }
  }

  // ---- atomic counters (embedded) ----

  await test('incr/decr from missing, floats, TTL preserved', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    assert.strictEqual(db.incr('c'), 1);
    assert.strictEqual(db.incr('c', 4), 5);
    assert.strictEqual(db.decr('c', 2), 3);
    assert.strictEqual(db.incr('f', 0.5), 0.5);
    db.set('t', 10, 5000);
    assert.strictEqual(db.incr('t'), 11);
    assert.ok(db.ttl('t') > 1000 && db.ttl('t') <= 5000, 'TTL preserved across incr');
    db.set('s', 'nope', 5000);
    assert.throws(() => db.incr('s'), /numeric/);
    assert.throws(() => db.incr('c', NaN), /finite/);
    db.close();
  });

  // ---- batch ops (embedded) ----

  await test('mget/mset round trip with TTLs', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    assert.strictEqual(db.mset([
      { key: 'a', value: 1 },
      { key: 'b', value: { posts: [1, 2] }, ttlMs: 5000 },
    ]), 2);
    assert.deepStrictEqual(db.mget(['a', 'b', 'missing']), [1, { posts: [1, 2] }, undefined]);
    assert.ok(db.ttl('b') > 0 && db.ttl('b') <= 5000);
    db.close();
  });

  // ---- local pub/sub ----

  await test('local publish/subscribe/unsubscribe', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const seen = [];
    const unsub = db.subscribe('presence', (ch, msg) => seen.push([ch, msg]));
    assert.strictEqual(db.publish('presence', '{"u":"a"}'), 1);
    assert.strictEqual(db.publish('nobody', 'x'), 0);
    assert.deepStrictEqual(seen, [['presence', '{"u":"a"}']]);
    unsub();
    assert.strictEqual(db.publish('presence', 'y'), 0);
    // Throwing listener never breaks the rest.
    db.subscribe('t', () => { throw new Error('bad'); });
    let ok = false;
    db.subscribe('t', () => { ok = true; });
    assert.strictEqual(db.publish('t', 'm'), 2);
    assert.strictEqual(ok, true);
    db.close();
  });

  // ---- persistence ----

  await test('snapshot save/load round trip (expired skipped)', async () => {
    const dir = tmpDir();
    const snap = path.join(dir, 'snapshot.json');
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('keep', { a: 1 }, 60000);
    db.set('gone', 1, 10);
    await sleep(30);
    const saved = await saveSnapshot(db, snap);
    assert.strictEqual(saved, 1, 'only live entries saved');
    const db2 = new YASD({ sweepIntervalMs: 0 });
    const loaded = await loadSnapshot(db2, snap);
    assert.strictEqual(loaded, 1);
    assert.deepStrictEqual(db2.get('keep'), { a: 1 });
    assert.strictEqual(db2.get('gone'), undefined);
    assert.strictEqual(await loadSnapshot(db2, path.join(dir, 'nope.json')), 0, 'missingOk');
    db.close();
    db2.close();
  });

  await test('AOF append + replay', async () => {
    const dir = tmpDir();
    const aofPath = path.join(dir, 'test.aof');
    const log = new AofLog(aofPath);
    log.append({ op: 'set', key: 'a', value: 1 });
    log.append({ op: 'incr', key: 'n', by: 2 });
    log.append({ op: 'mset', entries: [{ key: 'm', value: [1] }] });
    log.append({ op: 'expire', key: 'a', ttlMs: 5000 });
    log.append({ op: 'persist', key: 'a' });
    log.append({ op: 'del', keys: ['zz'] });
    log.append({ op: 'clear', prefix: 'zz' });
    fs.appendFileSync(aofPath, 'not json {{{'); // torn tail line is skipped
    const db = new YASD({ sweepIntervalMs: 0 });
    const applied = await log.replay(db);
    assert.strictEqual(applied, 7);
    assert.strictEqual(db.get('a'), 1);
    assert.strictEqual(db.ttl('a'), -1, 'persist replayed');
    assert.strictEqual(db.get('n'), 2);
    assert.deepStrictEqual(db.get('m'), [1]);
    db.close();
  });

  // ---- protocol codec ----

  await test('protocol encode/decode incl. partial chunks', async () => {
    const { encodeCommand, RespDecoder, requestArgv } = mod;
    const dec = new RespDecoder();
    const wire = encodeCommand(['SET', 'k', '{"a":1}', 'PX', '5000']);
    const half = Math.floor(wire.length / 2);
    assert.deepStrictEqual(dec.push(wire.slice(0, half)), []);
    const out = dec.push(wire.slice(half));
    assert.strictEqual(out.length, 1);
    assert.deepStrictEqual(requestArgv(out[0]), ['SET', 'k', '{"a":1}', 'PX', '5000']);
    // Replies: pipelined simple + int + nil bulk in one chunk.
    const { encodeReply } = mod;
    const buf = Buffer.concat([
      encodeReply({ kind: 'simple', value: 'OK' }),
      encodeReply({ kind: 'int', value: 3 }),
      encodeReply({ kind: 'bulk', value: null }),
      encodeReply({ kind: 'array', items: [{ kind: 'bulk', value: 'x' }, null] }),
    ]);
    const replies = new RespDecoder().push(buf);
    assert.strictEqual(replies.length, 4);
    // No null array elements on the wire: nil arrives as nil bulk.
    assert.deepStrictEqual(replies[3], { kind: 'array', items: [{ kind: 'bulk', value: 'x' }, { kind: 'bulk', value: null }] });
  });

  await test('CACHE_URL parsing + fromEnv', async () => {
    assert.deepStrictEqual(parseCacheUrl('yasd://cache.internal:7379?poolSize=8'), {
      host: 'cache.internal', port: 7379, poolSize: 8,
    });
    assert.deepStrictEqual(parseCacheUrl('yasd://127.0.0.1'), { host: '127.0.0.1', port: 7379 });
    assert.throws(() => parseCacheUrl('http://x'), /invalid CACHE_URL/);
    const c = YasdClient.fromEnv({ CACHE_URL: 'yasd://127.0.0.1:7379?poolSize=2' });
    assert.deepStrictEqual(c.endpoint, { host: '127.0.0.1', port: 7379 });
    await c.close();
  });

  // ---- server + client end to end ----

  await test('server/client: ping, kv, ttl, counters, batch', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port, poolSize: 2 });
    await client.connect();

    assert.strictEqual(await client.ping(), 'PONG');
    assert.strictEqual(await client.ping('hi'), 'hi');

    const feed = { posts: [{ id: 1 }] };
    assert.strictEqual(await client.set('feeds:home', feed, 15000), 'OK');
    assert.deepStrictEqual(await client.get('feeds:home'), feed);
    assert.strictEqual(await client.get('missing'), undefined);

    const t = await client.ttl('feeds:home');
    assert.ok(t > 0 && t <= 15000, `ttl, got ${t}`);
    assert.strictEqual(await client.expire('feeds:home', 60000), true);
    assert.ok((await client.ttl('feeds:home')) > 30000);
    assert.strictEqual(await client.persist('feeds:home'), true);
    assert.strictEqual(await client.ttl('feeds:home'), -1);
    assert.strictEqual(await client.expire('missing', 100), false);

    assert.strictEqual(await client.incr('likes:1'), 1);
    assert.strictEqual(await client.incr('likes:1', 4), 5);
    assert.strictEqual(await client.decr('likes:1', 2), 3);

    assert.strictEqual(await client.mset([{ key: 'm1', value: 1 }, { key: 'm2', value: [2] }]), 'OK');
    assert.deepStrictEqual(await client.mget(['m1', 'm2', 'nope']), [1, [2], undefined]);

    assert.strictEqual(await client.del('m1', 'm2'), 2);
    await client.set('tmp:1', 1);
    await client.set('tmp:2', 2);
    assert.strictEqual(await client.clearPrefix('tmp'), 2);

    const info = await client.info();
    assert.strictEqual(info.status, 'ok');
    assert.ok(info.entries >= 2, 'info reports entries');

    await client.close();
    await server.close();
  });

  await test('server/client: pipelined batch under pool', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port, poolSize: 2 });
    await client.connect();
    await Promise.all(Array.from({ length: 30 }, (_, i) => client.set(`p:${i}`, i)));
    const got = await Promise.all(Array.from({ length: 30 }, (_, i) => client.get(`p:${i}`)));
    assert.deepStrictEqual(got, Array.from({ length: 30 }, (_, i) => i));
    await client.close();
    await server.close();
  });

  await test('server/client: pub/sub + invalidation event', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const sub = new YasdClient({ host: '127.0.0.1', port, poolSize: 1 });
    const pub = new YasdClient({ host: '127.0.0.1', port, poolSize: 1 });
    await sub.connect();
    await pub.connect();

    const received = [];
    const unsub = await sub.subscribe('presence', (ch, msg) => received.push([ch, msg]));
    assert.strictEqual(await pub.publish('presence', '{"u":"bob"}'), 1);
    await sleep(100);
    assert.deepStrictEqual(received, [['presence', '{"u":"bob"}']]);

    // Server auto-publishes invalidations other replicas can consume.
    const invalid = [];
    const unsub2 = await sub.subscribe('__yasd__:invalidate', (ch, msg) => invalid.push(JSON.parse(msg)));
    await pub.set('feeds:x', { p: [] });
    await sleep(100);
    assert.ok(invalid.some(e => e.event === 'set' && e.key === 'feeds:x'), `invalidate, got ${JSON.stringify(invalid)}`);

    await unsub();
    await unsub2();
    assert.strictEqual(await pub.publish('presence', 'late'), 0);
    await sub.close();
    await pub.close();
    await server.close();
  });

  await test('server: /healthz + SAVE/LOAD + graceful close', async () => {
    const dir = tmpDir();
    const snap = path.join(dir, 's.json');
    const aof = path.join(dir, 'a.aof');
    const server = new YasdServer({
      host: '127.0.0.1', port: 0, snapshotPath: snap, aofPath: aof, autoSaveMs: 20,
    });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();
    await client.set('k', { v: 1 }, 60000);

    const health = await new Promise((resolve, reject) => {
      http.get({ host: '127.0.0.1', port, path: '/healthz', timeout: 2000 }, res => {
        let body = '';
        res.on('data', c => { body += c; });
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(body) }));
      }).on('error', reject);
    });
    assert.strictEqual(health.status, 200);
    assert.strictEqual(health.body.status, 'ok');
    assert.ok(health.body.entries >= 1);
    assert.deepStrictEqual(await client.healthcheck().then(h => h.status), 'ok');

    assert.strictEqual(await client.save(), 'OK');
    assert.ok(fs.existsSync(snap), 'snapshot written');
    await client.set('k2', 2, 60000);
    const loaded = await client.load();
    assert.ok(/^OK \d+/.test(loaded), `LOAD reply, got ${loaded}`);
    assert.strictEqual(await client.get('k2'), undefined, 'LOAD replaced state');
    assert.deepStrictEqual(await client.get('k'), { v: 1 });

    await client.close();
    await server.close(); // graceful: final SAVE, sockets drained
    assert.ok(fs.existsSync(snap), 'shutdown SAVE kept snapshot');
    // Restart restores snapshot + replays AOF.
    const server2 = new YasdServer({ host: '127.0.0.1', port: 0, snapshotPath: snap, aofPath: aof });
    await server2.start();
    assert.deepStrictEqual(server2.cache.get('k'), { v: 1 });
    await server2.close();
  });

  await test('server rejects bad input without dropping the connection', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();
    await assert.rejects(client.set('k', undefined), /undefined/);
    await assert.rejects(client.expire('k', NaN), /finite/);
    // Unknown command over a raw socket -> error reply, connection stays up.
    const net = require('net');
    const raw = await new Promise((resolve, reject) => {
      const lines = [];
      const sock = net.createConnection({ host: '127.0.0.1', port }, () => {
        sock.write('*1\r\n$7\r\nNOPECMD\r\n');
      });
      sock.on('data', chunk => {
        lines.push(chunk.toString());
        if (lines.join('').split('\r\n').length >= 2) {
          sock.write('*1\r\n$4\r\nPING\r\n');
        }
        const text = lines.join('');
        if (/^-ERR/.test(text) && /^\+PONG/m.test(text)) {
          sock.destroy();
          resolve(text);
        }
      });
      sock.on('error', reject);
      setTimeout(() => reject(new Error('raw socket test timed out: ' + lines.join(''))), 3000);
    });
    assert.ok(/unknown command/i.test(raw), `error reply, got ${raw}`);
    assert.strictEqual(await client.ping(), 'PONG', 'client connection still usable');
    await client.close();
    await server.close();
  });

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(`P1 tests completed: ${passed + failed}`);
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  console.log('='.repeat(50));

  if (failed > 0) {
    process.exit(1);
  }
}

runTests().catch(err => {
  console.error(err);
  process.exit(1);
});
