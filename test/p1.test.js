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
    fs.appendFileSync(aofPath, '{"version":1,"seq":8,"op":{"op":"set","key":"partial"')
    const db = new YASD({ sweepIntervalMs: 0 });
    const applied = await log.replay(db);
    assert.strictEqual(applied, 7);
    assert.strictEqual(log.recoveryState, 'torn-tail')
    assert.strictEqual(db.get('a'), 1);
    assert.strictEqual(db.ttl('a'), -1, 'persist replayed');
    assert.strictEqual(db.get('n'), 2);
    assert.deepStrictEqual(db.get('m'), [1]);
    db.close();
  })

  await test('AOF replay stops at middle corruption and exposes recovery state', async () => {
    const dir = tmpDir()
    const aofPath = path.join(dir, 'corrupt.aof')
    const source = new AofLog(aofPath)
    source.append({ op: 'set', key: 'before', value: 1 })
    source.append({ op: 'set', key: 'after', value: 2 })
    const records = fs.readFileSync(aofPath, 'utf8').trim().split('\n')
    fs.writeFileSync(aofPath, `${records[0]}\nnot json\n${records[1]}\n`)

    const log = new AofLog(aofPath)
    const db = new YASD({ sweepIntervalMs: 0 })
    assert.strictEqual(await log.replay(db), 1)
    assert.strictEqual(db.get('before'), 1)
    assert.strictEqual(db.get('after'), undefined)
    assert.strictEqual(log.recoveryState, 'corrupt')
    assert.match(log.recoveryError, /line 2/)
    assert.throws(() => log.append({ op: 'set', key: 'blocked', value: true }), /AOF is corrupt/)

    const server = new YasdServer({ aofPath, loadOnStart: false })
    assert.strictEqual(server.info().aofRecoveryState, 'corrupt')
    assert.match(server.info().aofRecoveryError, /line 2/)
    db.close()
    await server.close()
  })

  await test('AOF replay preserves absolute TTL deadlines', async () => {
    const dir = tmpDir();
    const aofPath = path.join(dir, 'ttl.aof');
    const deadline = Date.now() + 120;
    const log = new AofLog(aofPath);
    log.append({ op: 'set', key: 'ttl', value: 1, expiresAt: deadline });
    await sleep(40);

    const db = new YASD({ sweepIntervalMs: 0 });
    assert.strictEqual(await log.replay(db), 1);
    const afterReplay = Date.now();
    const remaining = db.ttl('ttl');
    assert.ok(remaining > 0 && remaining <= deadline - afterReplay + 5, 'TTL was not restarted from replay time');
    await sleep(100);
    assert.strictEqual(db.get('ttl'), undefined, 'original deadline still wins after replay');
    db.close();
  });

  await test('snapshot/AOF recovery skips superseded records after crash', async () => {
    const dir = tmpDir();
    const snap = path.join(dir, 'recovery-snapshot.json');
    const aofPath = path.join(dir, 'recovery.aof');
    const log = new AofLog(aofPath);
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('counter', 1);
    db.incr('counter');
    db.incr('counter');
    log.append({ op: 'incr', key: 'counter', by: 1 });
    const snapshotSeq = log.sequence;
    await saveSnapshot(db, snap, { aofSeq: snapshotSeq });

    db.incr('counter');
    log.append({ op: 'incr', key: 'counter', by: 1 });

    const recovered = new YASD({ sweepIntervalMs: 0 });
    const metadata = {};
    await loadSnapshot(recovered, snap, { metadata });
    assert.strictEqual(metadata.aofSeq, snapshotSeq);
    assert.strictEqual(await log.replay(recovered, metadata.aofSeq), 1);
    assert.strictEqual(recovered.get('counter'), 4, 'old AOF record was not replayed twice');

    log.rotateAfter(snapshotSeq);
    const rotated = new YASD({ sweepIntervalMs: 0 });
    const rotatedMetadata = {};
    await loadSnapshot(rotated, snap, { metadata: rotatedMetadata });
    assert.strictEqual(await log.replay(rotated, rotatedMetadata.aofSeq), 1);
    assert.strictEqual(rotated.get('counter'), 4, 'rotated AOF kept the post-snapshot tail');
    db.close();
    recovered.close();
    rotated.close();
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

  await test('protocol decoder enforces frame, bulk, args, depth, and buffer limits', async () => {
    const { RespDecoder } = mod;
    assert.throws(() => new RespDecoder({ maxFrameBytes: 4 }).push('+OK\r\n'), /frame exceeds/);
    assert.throws(() => new RespDecoder({ maxBulkBytes: 3 }).push('$4\r\n1234\r\n'), /bulk length exceeds/);
    assert.throws(() => new RespDecoder({ maxArguments: 1 }).push('*2\r\n$1\r\na\r\n$1\r\nb\r\n'), /array length exceeds/);
    assert.throws(() => new RespDecoder({ maxDepth: 1 }).push('*1\r\n*1\r\n$1\r\na\r\n'), /nesting exceeds/);
    assert.throws(() => new RespDecoder({ maxBufferedBytes: 4 }).push('*9999'), /buffered bytes exceeds/);
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

  await test('server/client: dead pooled connections are removed and replenished', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 })
    await server.start()
    const { port } = server.address()
    const client = new YasdClient({ host: '127.0.0.1', port, poolSize: 2 })
    await client.connect()

    for (let i = 0; i < 3; i++) {
      client.pool[0].socket.destroy()
      await client.connect()
      await client.ping()
      assert.strictEqual(client.pool.length, 2, 'pool stays bounded after replacement')
    }

    await client.close()
    await server.close()
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
    const receivedSecond = []
    const [unsub, unsubSecond] = await Promise.all([
      sub.subscribe('presence', (ch, msg) => received.push([ch, msg])),
      sub.subscribe('presence', (ch, msg) => receivedSecond.push([ch, msg])),
    ])
    assert.strictEqual(await pub.publish('presence', '{"u":"bob"}'), 1);
    await sleep(100);
    assert.deepStrictEqual(received, [['presence', '{"u":"bob"}']]);
    assert.deepStrictEqual(receivedSecond, [['presence', '{"u":"bob"}']]);

    sub.subSocket.destroy()
    await sleep(20)
    await sub.reconnectSubscriptions()
    assert.strictEqual(await pub.publish('presence', 'after-reconnect'), 1)
    await sleep(100)
    assert.deepStrictEqual(received[received.length - 1], ['presence', 'after-reconnect'])
    assert.deepStrictEqual(receivedSecond[receivedSecond.length - 1], ['presence', 'after-reconnect'])

    // Server auto-publishes invalidations other replicas can consume.
    const invalid = [];
    const unsub2 = await sub.subscribe('__yasd__:invalidate', (ch, msg) => invalid.push(JSON.parse(msg)));
    await pub.set('feeds:x', { p: [] });
    await sleep(100);
    assert.ok(invalid.some(e => e.event === 'set' && e.key === 'feeds:x'), `invalidate, got ${JSON.stringify(invalid)}`);

    await unsub();
    await unsubSecond()
    await unsub2();
    assert.strictEqual(await pub.publish('presence', 'late'), 0);
    await sub.close();
    await pub.close();
    await server.close();
  });

  await test('server: PERSIST, expiry, and LOAD publish invalidations', async () => {
    const dir = tmpDir()
    const snap = path.join(dir, 'invalidation-load.json')
    const server = new YasdServer({
      host: '127.0.0.1', port: 0, snapshotPath: snap,
      cache: { sweepIntervalMs: 10 },
    })
    await server.start()
    const { port } = server.address()
    const sub = new YasdClient({ host: '127.0.0.1', port })
    const client = new YasdClient({ host: '127.0.0.1', port })
    await sub.connect()
    await client.connect()

    const invalid = []
    const unsub = await sub.subscribe('__yasd__:invalidate', (ch, msg) => invalid.push(JSON.parse(msg)))
    await client.set('persist:key', 1, 1000)
    assert.strictEqual(await client.persist('persist:key'), true)
    await client.set('expire:key', 1, 50)
    await client.save()
    await client.set('stale:key', 2)
    await client.load()
    await sleep(150)

    assert.ok(invalid.some(e => e.event === 'persist' && e.key === 'persist:key'))
    assert.ok(invalid.some(e => e.event === 'expire' && e.key === 'expire:key'))
    assert.ok(invalid.some(e => e.event === 'load'))

    await unsub()
    await sub.close()
    await client.close()
    await server.close()
  })

  await test('server: CAS PX 0 stays absent after AOF replay', async () => {
    const dir = tmpDir();
    const aof = path.join(dir, 'cas-zero.aof');
    const server = new YasdServer({ host: '127.0.0.1', port: 0, aofPath: aof });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();
    await client.set('cas:zero', { old: true });
    assert.strictEqual(await client.cas('cas:zero', { old: true }, { new: true }, 0), true);
    assert.strictEqual(await client.get('cas:zero'), undefined);
    await client.close();
    await server.close();

    const records = fs.readFileSync(aof, 'utf8').trim().split('\n').map(line => JSON.parse(line));
    assert.ok(records.some(record => record.op?.op === 'del' && record.op.keys.includes('cas:zero')));
    const recovered = new YasdServer({ host: '127.0.0.1', port: 0, aofPath: aof });
    await recovered.start();
    assert.strictEqual(recovered.cache.get('cas:zero'), undefined);
    await recovered.close();
  });

  await test('server: AOF write failure is surfaced and rolled back', async () => {
    const dir = tmpDir();
    const aof = path.join(dir, 'write-failure.aof');
    const server = new YasdServer({ host: '127.0.0.1', port: 0, aofPath: aof });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    await client.set('seed', 0);
    fs.unlinkSync(aof);
    fs.mkdirSync(aof);
    await assert.rejects(client.set('lost', 1), /AOF write failed/);
    assert.strictEqual(server.cache.get('lost'), undefined);
    const info = await client.info();
    assert.strictEqual(info.aofEnabled, true);
    assert.strictEqual(info.aofDegraded, true);
    assert.ok(info.aofLastError);

    await client.close();
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

  await test('server serializes concurrent SAVE and LOAD calls', async () => {
    const dir = tmpDir();
    const snap = path.join(dir, 'serialized.json');
    const server = new YasdServer({
      snapshotPath: snap, loadOnStart: false, saveOnShutdown: false,
    });
    server.cache.set('old', 1);
    await server.save();

    server.cache.set('new', 2);
    const loading = server.load();
    const saving = server.save();
    await Promise.all([loading, saving]);

    const restored = new YASD({ sweepIntervalMs: 0 });
    await loadSnapshot(restored, snap, { missingOk: false });
    assert.strictEqual(restored.get('old'), 1);
    assert.strictEqual(restored.get('new'), undefined);

    restored.close();
    await server.close();
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
