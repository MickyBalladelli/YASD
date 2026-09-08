#!/usr/bin/env node
// Transaction tests: embedded KVTransaction (WATCH/MULTI/EXEC) plus the
// server/client protocol (WATCH/UNWATCH/MULTI/EXEC/DISCARD).
// Run: node test/transactions.test.js (needs the built dist + localhost).

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

const { YASD, YasdServer, YasdClient } = mod;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function tmpDir() {
  const dir = path.join(os.tmpdir(), `yasd-tx-${process.pid}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function runTests() {
  console.log('Starting YASD transaction tests...\n');

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

  // ---- embedded: basic commit ----

  await test('embedded: queue writes, exec applies atomically', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const tx = db.multi();
    tx.set('a', 1).set('b', { posts: [1] }).incr('c', 2);
    assert.strictEqual(tx.queued, 3);
    const results = tx.exec();
    assert.deepStrictEqual(results, [1, { posts: [1] }, 2]);
    assert.strictEqual(db.get('a'), 1);
    assert.deepStrictEqual(db.get('b'), { posts: [1] });
    assert.strictEqual(db.get('c'), 2);
    assert.strictEqual(tx.finished, true);
    db.close();
  });

  await test('embedded: read-modify-write commits when unwatched keys change', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('likes:1', 10);
    const tx = db.multi();
    tx.watch('likes:1');
    const likes = tx.get('likes:1');
    db.set('unrelated', 'x'); // untouched key must not abort
    tx.set('likes:1', likes + 1);
    const results = tx.exec();
    assert.deepStrictEqual(results, [11]);
    assert.strictEqual(db.get('likes:1'), 11);
    db.close();
  });

  // ---- embedded: conflicts ----

  await test('embedded: watched modification aborts (null, nothing applied)', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('k', 1);
    const tx = db.multi();
    tx.watch('k');
    assert.strictEqual(tx.get('k'), 1);
    db.set('k', 2); // external write between watch and exec
    tx.set('k', 99);
    tx.set('other', 'x');
    assert.strictEqual(tx.exec(), null);
    assert.strictEqual(db.get('k'), 2, 'aborted write not applied');
    assert.strictEqual(db.get('other'), undefined, 'whole batch rolled back');
    db.close();
  });

  await test('embedded: delete/recreate and expiry bump versions', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const v0 = db.keyVersion('nada');
    assert.strictEqual(v0, 0);
    db.set('t', 1);
    assert.ok(db.keyVersion('t') > 0);
    const v1 = db.keyVersion('t');
    db.del('t');
    assert.ok(db.keyVersion('t') > v1, 'delete bumps version');
    db.set('e', 1, 20);
    const ve = db.keyVersion('e');
    await sleep(40);
    assert.strictEqual(db.get('e'), undefined, 'expired');
    assert.ok(db.keyVersion('e') > ve, 'lazy expiry bumps version');
    db.close();
  });

  await test('embedded: unwatch clears conflict detection', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('k', 1);
    const tx = db.multi();
    tx.watch('k');
    tx.unwatch();
    db.set('k', 2);
    tx.set('k', 3);
    assert.deepStrictEqual(tx.exec(), [3]);
    assert.strictEqual(db.get('k'), 3);
    db.close();
  });

  // ---- embedded: lifecycle + validation ----

  await test('embedded: discard drops writes, reuse after finish throws', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const tx = db.multi();
    tx.set('a', 1);
    tx.discard();
    assert.strictEqual(tx.finished, true);
    assert.strictEqual(db.get('a'), undefined);
    assert.throws(() => tx.exec(), /finished/);
    assert.throws(() => tx.set('b', 2), /finished/);
    assert.throws(() => tx.discard(), /finished/);
    const empty = db.multi().exec();
    assert.deepStrictEqual(empty, [], 'empty tx commits to []');
    db.close();
  });

  await test('embedded: queue-time validation fails fast', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const tx = db.multi();
    assert.throws(() => tx.set('k', undefined), /undefined/);
    assert.throws(() => tx.set('k', 1, NaN), /finite/);
    assert.throws(() => tx.incr('k', Infinity), /finite/);
    assert.strictEqual(tx.queued, 0, 'invalid ops never queue');
    assert.deepStrictEqual(tx.exec(), []);
    db.close();
  });

  await test('embedded: commit-time failure rolls back (all-or-nothing)', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('s', 'not-a-number');
    const tx = db.multi();
    tx.set('newkey', 1);
    tx.incr('s'); // fails at commit: non-numeric live value
    assert.throws(() => tx.exec(), /numeric/);
    assert.strictEqual(db.get('newkey'), undefined, 'earlier op rolled back');
    assert.strictEqual(db.get('s'), 'not-a-number');
    db.close();
  });

  await test('embedded: cas/mset/clearPrefix/expire/persist inside tx', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('c', 1, 60000);
    db.set('ns:1', 'a');
    db.set('ns:2', 'b');
    const tx = db.multi();
    tx.cas('c', 1, 2);
    tx.mset([{ key: 'm1', value: 1 }, { key: 'm2', value: 2 }]);
    tx.clearPrefix('ns');
    tx.persist('c');
    const results = tx.exec();
    assert.deepStrictEqual(results, [true, 2, 2, true]);
    assert.strictEqual(db.get('c'), 2);
    assert.strictEqual(db.ttl('c'), -1);
    assert.strictEqual(db.get('ns:1'), undefined);
    assert.deepStrictEqual(db.mget(['m1', 'm2']), [1, 2]);
    db.close();
  });

  // ---- embedded: runTransaction helper ----

  await test('embedded: runTransaction commits first try', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const out = await db.runTransaction(['likes:9'], async tx => {
      const cur = tx.get('likes:9') ?? 0;
      tx.set('likes:9', cur + 1);
      return 'bumped';
    });
    assert.deepStrictEqual(out, { committed: true, attempts: 1, results: [1], value: 'bumped' });
    assert.strictEqual(db.get('likes:9'), 1);
    db.close();
  });

  await test('embedded: runTransaction retries on conflict, then commits', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('rk', 'v0');
    let calls = 0;
    const out = await db.runTransaction(['rk'], async tx => {
      calls++;
      const cur = tx.get('rk');
      if (calls === 1) db.set('rk', 'poison'); // conflict the first attempt
      tx.set('rk', `${cur}-w${calls}`);
      return calls;
    });
    assert.strictEqual(out.committed, true);
    assert.strictEqual(out.attempts, 2);
    assert.strictEqual(out.value, 2);
    assert.strictEqual(db.get('rk'), 'poison-w2');
    db.close();
  });

  await test('embedded: runTransaction gives up after maxRetries', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('hot', 0);
    const out = await db.runTransaction(['hot'], async tx => {
      tx.get('hot');
      db.incr('hot'); // always conflict
      tx.set('hot', 999);
      return 'x';
    }, 2);
    assert.strictEqual(out.committed, false);
    assert.strictEqual(out.attempts, 3, '1 initial + 2 retries');
    assert.notStrictEqual(db.get('hot'), 999);
    db.close();
  });

  await test('embedded: runTransaction rethrows fn errors, voluntary discard aborts', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    await assert.rejects(
      db.runTransaction(['k'], async tx => {
        tx.set('k', 1);
        throw new Error('boom');
      }),
      /boom/
    );
    assert.strictEqual(db.get('k'), undefined, 'throw discards queued writes');
    const out = await db.runTransaction(['k'], async tx => {
      if (tx.get('k') === undefined) {
        tx.set('k', 'fresh');
        return 'miss';
      }
      tx.discard();
      return 'hit';
    });
    assert.strictEqual(out.committed, true);
    assert.strictEqual(out.value, 'miss');
    const out2 = await db.runTransaction(['k'], async tx => {
      if (tx.get('k') === undefined) {
        tx.set('k', 'fresh');
        return 'miss';
      }
      tx.discard();
      return 'hit';
    });
    assert.strictEqual(out2.committed, false);
    assert.strictEqual(out2.value, 'hit');
    db.close();
  });

  // ---- protocol codec ----

  await test('protocol: nil array (*-1) round trip for aborted EXEC', async () => {
    const { encodeReply, RespDecoder } = mod;
    const wire = encodeReply({ kind: 'nil' });
    assert.strictEqual(wire.toString('utf8'), '*-1\r\n');
    const out = new RespDecoder().push(wire);
    assert.deepStrictEqual(out, [{ kind: 'nil' }]);
  });

  // ---- server/client end to end ----

  await test('server/client: WATCH/read/MULTI/write/EXEC commits', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    await client.set('likes:1', 10);
    const tx = client.multi();
    await tx.watch('likes:1');
    const cur = await tx.get('likes:1');
    await tx.set('likes:1', cur + 1);
    await tx.incr('writes');
    const results = await tx.exec();
    assert.deepStrictEqual(results, [11, 1]);
    assert.strictEqual(await client.get('likes:1'), 11);
    assert.strictEqual(tx.finished, true);

    await client.close();
    await server.close();
  });

  await test('server/client: watched modification aborts with null', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    await client.set('w', 1);
    const tx = client.multi();
    await tx.watch('w');
    await client.set('w', 2); // concurrent modification
    await tx.set('w', 99);
    assert.strictEqual(await tx.exec(), null);
    assert.strictEqual(await client.get('w'), 2, 'aborted write not applied');

    await client.close();
    await server.close();
  });

  await test('server/client: DISCARD drops the queue', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    const tx = client.multi();
    await tx.watch('d');
    await tx.set('d', 1);
    await tx.discard();
    assert.strictEqual(await client.get('d'), undefined);
    assert.strictEqual(tx.finished, true);

    await client.close();
    await server.close();
  });

  await test('server/client: runTransaction retries then commits', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    await client.set('rk', 'v0');
    let calls = 0;
    const out = await client.runTransaction(['rk'], async tx => {
      calls++;
      const cur = await tx.get('rk');
      if (calls === 1) await client.set('rk', 'poison');
      await tx.set('rk', `${cur}-w${calls}`);
      return calls;
    });
    assert.strictEqual(out.committed, true);
    assert.strictEqual(out.attempts, 2);
    assert.strictEqual(await client.get('rk'), 'poison-w2');

    await client.close();
    await server.close();
  });

  await test('server/client: EXEC commits are logged + invalidate like writes', async () => {
    const dir = tmpDir();
    const aof = path.join(dir, 'tx.aof');
    const server = new YasdServer({ host: '127.0.0.1', port: 0, aofPath: aof });
    await server.start();
    const { port } = server.address();
    const sub = new YasdClient({ host: '127.0.0.1', port, poolSize: 1 });
    const client = new YasdClient({ host: '127.0.0.1', port, poolSize: 1 });
    await sub.connect();
    await client.connect();

    const invalid = [];
    const unsub = await sub.subscribe('__yasd__:invalidate', (ch, msg) => invalid.push(JSON.parse(msg)));
    const tx = client.multi();
    await tx.set('tx:k', { v: 1 });
    await tx.incr('tx:n');
    const results = await tx.exec();
    assert.deepStrictEqual(results, ['OK', 1]);
    await sleep(100);
    assert.ok(invalid.some(e => e.event === 'set' && e.key === 'tx:k'), `invalidate, got ${JSON.stringify(invalid)}`);
    assert.ok(invalid.some(e => e.event === 'set' && e.key === 'tx:n'), `invalidate, got ${JSON.stringify(invalid)}`);
    const aofRaw = fs.readFileSync(aof, 'utf8');
    assert.ok(aofRaw.includes('"key":"tx:k"'), `AOF logs tx set, got ${aofRaw}`);
    assert.ok(aofRaw.includes('"op":"incr"'), `AOF logs tx incr, got ${aofRaw}`);

    await unsub();
    await sub.close();
    await client.close();
    await server.close();
  });

  await test('server: tx misuse is rejected, connection stays usable', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const net = require('net');

    const convo = await new Promise((resolve, reject) => {
      const chunks = [];
      const sock = net.createConnection({ host: '127.0.0.1', port }, () => {
        const { encodeCommand } = mod;
        sock.write(encodeCommand(['EXEC'])); // no MULTI
        sock.write(encodeCommand(['DISCARD'])); // no MULTI
        sock.write(encodeCommand(['MULTI']));
        sock.write(encodeCommand(['MULTI'])); // nested
        sock.write(encodeCommand(['NOSUCHCMD'])); // not queueable
        sock.write(encodeCommand(['SET', 'rk', '1'])); // QUEUED
        sock.write(encodeCommand(['DISCARD']));
        sock.write(encodeCommand(['GET', 'rk'])); // discarded -> nil
        sock.write(encodeCommand(['PING']));
      });
      sock.on('data', c => {
        chunks.push(c.toString());
        const text = chunks.join('');
        if ((text.match(/(\+OK|\+QUEUED|-ERR|\$-1|\+PONG)/g) || []).length >= 9) {
          sock.destroy();
          resolve(text);
        }
      });
      sock.on('error', reject);
      setTimeout(() => reject(new Error('raw socket test timed out: ' + chunks.join(''))), 3000);
    });
    assert.ok(/EXEC without MULTI/.test(convo), `EXEC guard, got ${convo}`);
    assert.ok(/DISCARD without MULTI/.test(convo), `DISCARD guard, got ${convo}`);
    assert.ok(/cannot nest/.test(convo), `nested MULTI guard, got ${convo}`);
    assert.ok(/not allowed inside MULTI/.test(convo), `queue guard, got ${convo}`);
    assert.ok(/QUEUED/.test(convo), `QUEUED reply, got ${convo}`);
    assert.ok(/\$-1/.test(convo), `discarded key still missing, got ${convo}`);
    assert.ok(/PONG/.test(convo), `connection alive, got ${convo}`);

    // WATCH/MULTI/EXEC abort over raw socket returns *-1.
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();
    await client.set('ab', 1);
    const abort = await new Promise((resolve, reject) => {
      const chunks = [];
      const sock = net.createConnection({ host: '127.0.0.1', port }, () => {
        const { encodeCommand } = mod;
        sock.write(encodeCommand(['WATCH', 'ab']));
      });
      sock.on('data', c => {
        chunks.push(c.toString());
        const text = chunks.join('');
        if (/\+OK/.test(text) && !sock.didPoison) {
          sock.didPoison = true;
          client.set('ab', 2).then(() => {
            const { encodeCommand } = mod;
            sock.write(encodeCommand(['MULTI']));
            sock.write(encodeCommand(['SET', 'ab', '99']));
            sock.write(encodeCommand(['EXEC']));
          });
        }
        if (/\*-1/.test(text)) {
          sock.destroy();
          resolve(text);
        }
      });
      sock.on('error', reject);
      setTimeout(() => reject(new Error('abort test timed out: ' + chunks.join(''))), 3000);
    });
    assert.ok(/\*-1/.test(abort), `nil array on abort, got ${abort}`);
    assert.strictEqual(await client.get('ab'), 2, 'aborted tx applied nothing');

    await client.close();
    await server.close();
  });

  await test('client: reads after MULTI are rejected, one-shot reuse throws', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    const tx = client.multi();
    await tx.set('q', 1); // auto-MULTI
    assert.strictEqual(tx.inMulti, true);
    await assert.rejects(tx.get('q'), /precede MULTI/);
    const results = await tx.exec();
    assert.deepStrictEqual(results, ['OK']);
    await assert.rejects(tx.exec(), /finished/);
    await assert.rejects(tx.set('q', 2), /finished/);

    await client.close();
    await server.close();
  });

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(`Transaction tests completed: ${passed + failed}`);
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
