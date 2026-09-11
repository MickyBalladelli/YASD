#!/usr/bin/env node
// KV / TTL / LRU regression tests for the P0 cache fast path.
// Run: node test/kv.test.js (no dependencies beyond the built dist).

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
const { YASD, parse } = mod;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTests() {
  console.log('Starting YASD KV tests...\n');

  let passed = 0;
  let failed = 0;

  async function test(name, fn) {
    try {
      await fn();
      console.log(`✓ ${name}`);
      passed++;
    } catch (error) {
      console.log(`✗ ${name}`);
      console.log(`  Error: ${error.message}`);
      failed++;
    }
  }

  // 1. KV fast path stores objects/arrays (Echo feeds) and set returns value.
  await test('KV stores objects, set returns value', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    const feed = { posts: [{ id: 1, text: 'hi' }], cursor: 'abc' };
    assert.strictEqual(db.set('feed:123', feed, 5000), feed);
    assert.deepStrictEqual(db.get('feed:123'), feed);
    db.close();
  });

  // 2. del semantics.
  await test('del returns true once, then false', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('k', 'v', 5000);
    assert.strictEqual(db.del('k'), true);
    assert.strictEqual(db.get('k'), undefined);
    assert.strictEqual(db.del('k'), false);
    db.close();
  });

  // 3. ttlMs is required to be valid: no undefined -> NaN silent persist bug.
  await test('invalid ttlMs throws (NaN/Infinity/negative)', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    assert.throws(() => db.set('k', 'v', NaN), /finite/);
    assert.throws(() => db.set('k', 'v', Infinity), /finite/);
    assert.throws(() => db.set('k', 'v', -1), />= 0/);
    assert.throws(() => db.expire('k', NaN), /finite/);
    db.close();
  });

  // 4. Per-namespace TTL defaults (feeds ~15s, popular 60s, channels 30s).
  await test('namespace TTL defaults apply', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('feeds:home', [1, 2, 3]);
    const feeds = db.ttl('feeds:home');
    assert.ok(feeds > 10000 && feeds <= 15000, `feeds ttl ~15s, got ${feeds}`);
    db.set('popular:home', [1]);
    const popular = db.ttl('popular:home');
    assert.ok(popular > 55000 && popular <= 60000, `popular ttl ~60s, got ${popular}`);
    db.set('plain', 1);
    assert.strictEqual(db.ttl('plain'), -1, 'no default => persists (-1)');
    assert.strictEqual(db.ttl('missing'), -2, 'missing => -2');
    db.close();
  });

  // 5. TTL / EXPIRE / PERSIST semantics.
  await test('ttl/expire/persist', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('s', 'v', 50);
    const remaining = db.ttl('s');
    assert.ok(remaining >= 0 && remaining <= 50, `countdown, got ${remaining}`);
    assert.strictEqual(db.expire('missing', 100), false);
    assert.strictEqual(db.expire('s', 5000), true);
    assert.ok(db.ttl('s') > 1000, 'expire extends TTL');
    assert.strictEqual(db.persist('s'), true);
    assert.strictEqual(db.ttl('s'), -1, 'persisted');
    assert.strictEqual(db.persist('missing'), false);
    db.close();
  });

  // 6. Lazy expiry on get.
  await test('lazy expiry on get', async () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('fast', 1, 20);
    assert.strictEqual(db.get('fast'), 1);
    await sleep(40);
    assert.strictEqual(db.get('fast'), undefined, 'expired on get');
    assert.strictEqual(db.ttl('fast'), -2);
    db.close();
  });

  // 7. Background sweeper removes expired keys.
  await test('sweeper removes expired keys', async () => {
    const db = new YASD({ sweepIntervalMs: 10 });
    db.set('s1', 1, 10);
    db.set('s2', 2, 10);
    db.set('keep', 3, 60000);
    await sleep(60);
    assert.strictEqual(db.get('keep'), 3);
    assert.strictEqual(db.get('s1'), undefined);
    assert.strictEqual(db.get('s2'), undefined);
    assert.ok(db.cacheStats().expiries >= 2, 'expiries counted');
    db.close();
  });

  // 8. clearPrefix namespace invalidation (Echo cacheClear seam).
  await test('clearPrefix invalidates a namespace', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.set('feed:1', 1);
    db.set('feed:2', 2);
    db.set('channels:all', 3);
    db.set('other', 4);
    assert.strictEqual(db.clearPrefix('feed'), 2);
    assert.strictEqual(db.get('feed:1'), undefined);
    assert.strictEqual(db.get('channels:all'), 3);
    assert.strictEqual(db.get('other'), 4);
    assert.strictEqual(db.clearPrefix('nothing'), 0);
    db.close();
  });

  // 9. LRU (not FIFO) eviction under maxEntries.
  await test('LRU eviction under maxEntries', () => {
    const db = new YASD({ maxEntries: 3, sweepIntervalMs: 0 });
    db.set('a', 1);
    db.set('b', 2);
    db.set('c', 3);
    db.get('a'); // touch a => b becomes LRU victim
    db.set('d', 4);
    assert.strictEqual(db.get('b'), undefined, 'LRU victim evicted');
    assert.strictEqual(db.get('a'), 1, 'recently used survives');
    assert.strictEqual(db.get('c'), 3);
    assert.strictEqual(db.get('d'), 4);
    assert.strictEqual(db.cacheStats().evictions, 1);
    db.close();
  });

  // 10. Size-aware eviction under maxBytes.
  await test('size-aware eviction under maxBytes', () => {
    const db = new YASD({ maxBytes: 300, sweepIntervalMs: 0 });
    db.set('big1', 'x'.repeat(200));
    db.set('big2', 'y'.repeat(200));
    assert.strictEqual(db.get('big1'), undefined, 'oversized LRU entry evicted');
    assert.strictEqual(db.get('big2'), 'y'.repeat(200));
    db.close();
  });

  // 11. Index-backed reads (=, IN, AND) agree with full scans.
  await test('indexed WHERE =, IN, AND', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.query('CREATE TABLE users (id int primary key, name string, age number)');
    const names = ['Ann', 'Bob', 'Cat', 'Dan', 'Eve'];
    for (let i = 1; i <= 200; i++) {
      db.query(`INSERT INTO users VALUES (${i}, '${names[i % 5]}', ${20 + (i % 30)})`);
    }
    const eq = db.query("SELECT * FROM users WHERE name = 'Ann'");
    assert.ok(eq.rows.length > 0, 'eq finds rows');
    assert.ok(eq.rows.every(r => r.name === 'Ann'), 'eq rows match');
    const inRes = db.query('SELECT * FROM users WHERE id IN (1, 2, 3)');
    assert.strictEqual(inRes.rows.length, 3, 'IN finds 3 rows');
    const andRes = db.query("SELECT * FROM users WHERE age = 25 AND name = 'Bob'");
    const scan = db.query('SELECT * FROM users').rows.filter(r => r.age === 25 && r.name === 'Bob');
    assert.strictEqual(andRes.rows.length, scan.length, 'AND matches full scan');
    assert.ok(andRes.rows.every(r => r.age === 25 && r.name === 'Bob'));
    db.close();
  });

  await test('WHERE preserves literal and column-reference identity', () => {
    const ast = parse("SELECT * FROM people WHERE name = 'age'");
    assert.strictEqual(ast.where.left.type, 'column_ref');
    assert.deepStrictEqual(ast.where.right, { type: 'literal', value: 'age' });

    const db = new YASD({ sweepIntervalMs: 0 });
    db.query('CREATE TABLE people (name string, age string)');
    db.query("INSERT INTO people VALUES ('age', '30'), ('bob', 'age'), ('age', 'age')");
    const literal = db.query("SELECT * FROM people WHERE name = 'age'");
    assert.deepStrictEqual(literal.rows.map(row => row.name), ['age', 'age']);
    const column = db.query('SELECT * FROM people WHERE name = age');
    assert.deepStrictEqual(column.rows.map(row => row.name), ['age']);
    db.close();
  });

  // 12. DELETE correctness: rows gone, indexes consistent afterwards.
  await test('delete then index reads stay correct', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.query('CREATE TABLE t (id int primary key, v string)');
    for (let i = 1; i <= 50; i++) {
      db.query(`INSERT INTO t VALUES (${i}, 'v${i % 3}')`);
    }
    const del = db.query('DELETE FROM t WHERE id = 7');
    assert.strictEqual(del.affectedRows, 1);
    assert.strictEqual(db.query('SELECT * FROM t WHERE id = 7').rows.length, 0);
    assert.strictEqual(db.query('SELECT * FROM t').rows.length, 49);
    const delMany = db.query("DELETE FROM t WHERE v = 'v1'");
    assert.ok(delMany.affectedRows > 0, 'bulk delete removes rows');
    const rest = db.query('SELECT * FROM t');
    assert.ok(rest.rows.every(r => r.v !== 'v1'), 'no deleted values remain');
    assert.strictEqual(db.query("SELECT * FROM t WHERE v = 'v0'").rows.length,
      rest.rows.filter(r => r.v === 'v0').length, 'index read matches scan after delete');
    db.close();
  });

  // 13. Unified parser path: operators the old duplicate got wrong.
  await test('parser handles >=, !=, BETWEEN, LIKE', () => {
    const db = new YASD({ sweepIntervalMs: 0 });
    db.query('CREATE TABLE u (id int, name string, age number)');
    db.query("INSERT INTO u VALUES (1, 'John', 30), (2, 'Jane', 25), (3, 'Bob', 35)");
    assert.strictEqual(db.query('SELECT * FROM u WHERE age >= 30').rows.length, 2);
    assert.strictEqual(db.query("SELECT * FROM u WHERE name != 'John'").rows.length, 2);
    assert.strictEqual(db.query('SELECT * FROM u WHERE age BETWEEN 24 AND 31').rows.length, 2);
    assert.strictEqual(db.query("SELECT * FROM u WHERE name LIKE 'J%'").rows.length, 2);
    db.close();
  });

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(`KV tests completed: ${passed + failed}`);
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
