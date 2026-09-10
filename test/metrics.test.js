#!/usr/bin/env node
// Metrics tests: KV counters (hits/misses/evictions/expiries/bytes),
// resetStats, SQL slow-query log, SlowLog unit behavior, and server INFO
// exposure (counters + slow-command log).
// Run: node test/metrics.test.js (needs the built dist + localhost).

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

const { YASD, YasdServer, YasdClient, SlowLog, serverOptionsFromEnv } = mod;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTests() {
  console.log('Starting YASD metrics tests...\n');

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

  await test('counters start at zero', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    assert.deepStrictEqual(db.cacheStats(), {
      hits: 0, misses: 0, evictions: 0, expiries: 0, entries: 0, bytes: 0,
    });
    db.close();
  });

  await test('hits/misses counted on get/mget/ttl', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('a', 1);
    db.set('b', 2);
    const afterSet = db.cacheStats();
    assert.strictEqual(afterSet.entries, 2);
    assert.ok(afterSet.bytes > 0, `bytes tracked, got ${afterSet.bytes}`);

    assert.strictEqual(db.get('a'), 1); // hit
    assert.strictEqual(db.get('nope'), undefined); // miss
    assert.deepStrictEqual(db.mget(['a', 'b', 'nope']), [1, 2, undefined]); // hit, hit, miss
    assert.strictEqual(db.ttl('nope'), -2); // miss
    assert.ok(db.ttl('a') === -1 || db.ttl('a') >= 0, 'live ttl read (no counter change)');

    const s = db.cacheStats();
    assert.strictEqual(s.hits, 3, `hits, got ${JSON.stringify(s)}`);
    assert.strictEqual(s.misses, 3, `misses, got ${JSON.stringify(s)}`);
    db.close();
  });

  await test('expiries counted on lazy expiry', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('fast', 1, 20);
    await sleep(50);
    assert.strictEqual(db.get('fast'), undefined);
    const s = db.cacheStats();
    assert.ok(s.expiries >= 1, `expiries, got ${JSON.stringify(s)}`);
    assert.strictEqual(s.entries, 0);
    db.close();
  });

  await test('evictions counted under maxEntries', async () => {
    const db = new YASD({ maxEntries: 2, sweepIntervalMs: 0 });
    db.set('x', 1);
    db.set('y', 2);
    db.set('z', 3);
    const s = db.cacheStats();
    assert.strictEqual(s.evictions, 1, `evictions, got ${JSON.stringify(s)}`);
    assert.strictEqual(s.entries, 2);
    db.close();
  });

  await test('resetStats zeroes counters, keeps data', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('a', 1);
    db.get('a');
    db.get('missing');
    db.resetStats();
    const s = db.cacheStats();
    assert.deepStrictEqual(
      { hits: s.hits, misses: s.misses, evictions: s.evictions, expiries: s.expiries },
      { hits: 0, misses: 0, evictions: 0, expiries: 0 }
    );
    assert.strictEqual(s.entries, 1, 'data kept');
    assert.strictEqual(db.get('a'), 1, 'value still readable');
    assert.strictEqual(db.cacheStats().hits, 1, 'counters resume after reset');
    db.close();
  });

  await test('SQL slow-query log: off by default, threshold + cap + clear', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.query('CREATE TABLE t (id int)');
    db.query('SELECT * FROM t');
    assert.deepStrictEqual(db.slowLog(), [], 'disabled by default');

    db.setSlowQueryThreshold(1e-9); // log everything
    db.query('SELECT * FROM t');
    let log = db.slowLog();
    assert.strictEqual(log.length, 1);
    assert.ok(log[0].sql.includes('SELECT'), `sql text kept, got ${JSON.stringify(log[0])}`);
    assert.ok(log[0].durationMs >= 0, 'duration measured');
    assert.ok(log[0].at > 0, 'timestamp set');

    for (let i = 0; i < 105; i++) db.query(`SELECT * FROM t WHERE id = ${i}`);
    log = db.slowLog();
    assert.strictEqual(log.length, 100, `capped at 100, got ${log.length}`);
    assert.ok(log[0].at >= log[log.length - 1].at, 'newest first');

    db.clearSlowLog();
    assert.deepStrictEqual(db.slowLog(), []);
    assert.throws(() => db.setSlowQueryThreshold(-1), />= 0/);
    assert.throws(() => db.setSlowQueryThreshold(NaN), />= 0/);
    db.close();
  });

  await test('SlowLog unit: threshold, boundary, cap, validation', async () => {
    const off = new SlowLog();
    assert.strictEqual(off.record('Q', 999), false);
    assert.strictEqual(off.size, 0);

    const log = new SlowLog(10);
    assert.strictEqual(log.threshold, 10);
    assert.strictEqual(log.record('fast', 5), false, 'below threshold skipped');
    assert.strictEqual(log.record('exact', 10), true, 'boundary (>=) logged');
    assert.strictEqual(log.record('slow', 25), true);
    const names = log.list().map(e => e.name);
    assert.deepStrictEqual(names, ['slow', 'exact'], 'newest first');
    log.clear();
    assert.strictEqual(log.size, 0);

    const tiny = new SlowLog(1e-9, 3);
    for (let i = 0; i < 5; i++) tiny.record(`q${i}`, 1);
    assert.strictEqual(tiny.size, 3, 'cap enforced');
    assert.strictEqual(tiny.list()[0].name, 'q4', 'newest first under cap');

    assert.throws(() => new SlowLog(-1), />= 0/);
    assert.throws(() => log.setThreshold(NaN), />= 0/);
  });

  await test('server INFO exposes counters; slow log off by default', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    await client.set('k', { v: 1 });
    await client.get('k');
    await client.get('missing');
    const info = await client.info();
    assert.strictEqual(info.status, 'ok');
    assert.ok(info.uptimeMs >= 0);
    assert.ok(info.connections >= 1);
    assert.ok(info.entries >= 1, `entries, got ${JSON.stringify(info)}`);
    assert.ok(info.bytes > 0, `bytes, got ${JSON.stringify(info)}`);
    assert.ok(info.hits >= 1, `hits, got ${JSON.stringify(info)}`);
    assert.ok(info.misses >= 1, `misses, got ${JSON.stringify(info)}`);
    assert.strictEqual(info.slowCommandMs, 0, 'slow log off by default');
    assert.deepStrictEqual(info.slowLog, [], 'no slow entries when off');

    await client.close();
    await server.close();
  });

  await test('server slow-command log records with tiny threshold', async () => {
    const server = new YasdServer({ host: '127.0.0.1', port: 0, slowCommandMs: 1e-9 });
    await server.start();
    const { port } = server.address();
    const client = new YasdClient({ host: '127.0.0.1', port });
    await client.connect();

    await client.set('s', 1);
    await client.get('s');
    await client.ping();
    const info = await client.info();
    assert.strictEqual(info.slowCommandMs, 1e-9);
    const names = info.slowLog.map(e => e.name);
    assert.ok(names.includes('SET'), `SET logged, got ${JSON.stringify(names)}`);
    assert.ok(names.includes('GET'), `GET logged, got ${JSON.stringify(names)}`);
    for (const e of info.slowLog) {
      assert.ok(e.durationMs >= 0, 'duration measured');
      assert.ok(e.at > 0, 'timestamp set');
      assert.ok(Number.isInteger(e.argc) && e.argc >= 0, `argc, got ${JSON.stringify(e)}`);
    }
    // Newest first — and INFO itself is excluded by construction: the reply
    // is serialized before the INFO entry is recorded.
    assert.strictEqual(info.slowLog[0].name, 'PING');

    server.clearSlowLog();
    assert.deepStrictEqual(server.slowLog(), []);
    server.setSlowCommandThreshold(0);
    await client.ping();
    assert.deepStrictEqual(server.slowLog(), [], 'disabled again');
    assert.throws(() => server.setSlowCommandThreshold(-1), />= 0/);

    await client.close();
    await server.close();
  });

  await test('serverOptionsFromEnv parses YASD_SLOW_COMMAND_MS', async () => {
    assert.strictEqual(serverOptionsFromEnv({}).slowCommandMs, undefined);
    assert.strictEqual(
      serverOptionsFromEnv({ YASD_SLOW_COMMAND_MS: '2.5' }).slowCommandMs,
      2.5
    );
    assert.throws(() => serverOptionsFromEnv({ YASD_SLOW_COMMAND_MS: 'nope' }), />= 0/);
    assert.throws(() => serverOptionsFromEnv({ YASD_SLOW_COMMAND_MS: '-1' }), />= 0/);
  });

  await test('server defaults to loopback', async () => {
    assert.strictEqual(serverOptionsFromEnv({}).host, '127.0.0.1')
    assert.strictEqual(new YasdServer().host, '127.0.0.1')
  })

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(`Metrics tests completed: ${passed + failed}`);
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
