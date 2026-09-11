#!/usr/bin/env node
// Query profiling tests: EXPLAIN plans for WHERE/ORDER BY/LIMIT paths
// (index-scan vs full-scan) and PROFILE runs (plan + timing + row counts).
// Run: node test/profiling.test.js (needs the built dist).

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

const { YASD } = mod;

function seed() {
  const db = new YASD({ sweepIntervalMs: 0 });
  db.query('CREATE TABLE users (id int primary key, name string, age number)');
  const names = ['Ann', 'Bob', 'Cat', 'Dan', 'Eve'];
  for (let i = 1; i <= 200; i++) {
    db.query(`INSERT INTO users VALUES (${i}, '${names[i % 5]}', ${20 + (i % 30)})`);
  }
  return db;
}

async function runTests() {
  console.log('Starting YASD profiling tests...\n');

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

  // ---- EXPLAIN: WHERE paths ----

  await test('explain: = and IN use index-scan', async () => {
    const db = seed();
    const eq = db.explain("SELECT * FROM users WHERE name = 'Ann'");
    assert.strictEqual(eq.statement, 'select');
    assert.strictEqual(eq.table, 'users');
    assert.strictEqual(eq.strategy, 'index-scan');
    assert.deepStrictEqual(eq.indexColumns, ['name']);
    assert.strictEqual(eq.tableRows, 200);

    const inPlan = db.explain('SELECT * FROM users WHERE id IN (1, 2, 3)');
    assert.strictEqual(inPlan.strategy, 'index-scan');
    assert.deepStrictEqual(inPlan.indexColumns, ['id']);
    db.close();
  });

  await test('explain: AND of indexable predicates uses index-scan', async () => {
    const db = seed();
    const plan = db.explain("SELECT * FROM users WHERE age = 25 AND name = 'Ann'");
    assert.strictEqual(plan.strategy, 'index-scan');
    assert.deepStrictEqual([...plan.indexColumns].sort(), ['age', 'name']);
    assert.strictEqual(db.query("SELECT * FROM users WHERE age = 25 AND name = 'Ann'").rows.length, 7);
    db.close();
  });

  await test('explain: range/LIKE/OR/mixed-AND fall back to full-scan', async () => {
    const db = seed();
    for (const sql of [
      'SELECT * FROM users WHERE age > 25',
      "SELECT * FROM users WHERE name LIKE 'A%'",
      "SELECT * FROM users WHERE name = 'Ann' OR age = 30",
      "SELECT * FROM users WHERE age > 25 AND name = 'Bob'",
      'SELECT * FROM users',
    ]) {
      const plan = db.explain(sql);
      assert.strictEqual(plan.strategy, 'full-scan', sql);
      assert.strictEqual(plan.indexColumns, undefined, sql);
    }
    db.close();
  });

  await test('explain: plans agree with execution (index results are honest)', async () => {
    const db = seed();
    const cases = [
      "SELECT * FROM users WHERE name = 'Ann'",
      'SELECT * FROM users WHERE id IN (1, 2, 3)',
      "SELECT * FROM users WHERE age = 25 AND name = 'Ann'",
    ];
    for (const sql of cases) {
      const plan = db.explain(sql);
      assert.strictEqual(plan.strategy, 'index-scan', sql);
      const res = db.query(sql);
      const scan = db.query('SELECT * FROM users').rows.filter(r => {
        if (sql.includes('Ann') && !sql.includes('age')) return r.name === 'Ann';
        if (sql.includes('IN')) return [1, 2, 3].includes(r.id);
        return r.age === 25 && r.name === 'Ann';
      });
      assert.strictEqual(res.rows.length, scan.length, `${sql}: same rows as full scan`);
      assert.ok(res.rows.length > 0, `${sql}: non-empty`);
    }
    db.close();
  });

  // ---- EXPLAIN: ORDER BY / LIMIT paths ----

  await test('explain: ORDER BY detail and defaults', async () => {
    const db = seed();
    const plain = db.explain('SELECT * FROM users');
    assert.strictEqual(plain.hasOrderBy, false);
    assert.strictEqual(plain.orderBy, undefined);

    const asc = db.explain('SELECT * FROM users ORDER BY age');
    assert.strictEqual(asc.hasOrderBy, true);
    assert.deepStrictEqual(asc.orderBy, { column: 'age', direction: 'asc' });

    const desc = db.explain('SELECT * FROM users ORDER BY age DESC');
    assert.deepStrictEqual(desc.orderBy, { column: 'age', direction: 'desc' });

    // Combined with a WHERE path: both halves reported.
    const combo = db.explain("SELECT * FROM users WHERE name = 'Ann' ORDER BY age DESC");
    assert.strictEqual(combo.strategy, 'index-scan');
    assert.deepStrictEqual(combo.orderBy, { column: 'age', direction: 'desc' });
    db.close();
  });

  await test('explain: LIMIT/OFFSET echoed, columns and table reported', async () => {
    const db = seed();
    const paged = db.explain('SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5');
    assert.strictEqual(paged.limit, 10);
    assert.strictEqual(paged.offset, 5);

    const noPage = db.explain('SELECT * FROM users');
    assert.strictEqual(noPage.limit, undefined);
    assert.strictEqual(noPage.offset, undefined);

    const cols = db.explain('SELECT name, age FROM users WHERE id = 1');
    assert.deepStrictEqual(cols.columns, ['name', 'age']);
    assert.strictEqual(cols.strategy, 'index-scan');

    const star = db.explain('SELECT * FROM users');
    assert.strictEqual(star.columns, '*');
    assert.strictEqual(star.table, 'users');
    assert.strictEqual(star.tableRows, 200);
    db.close();
  });

  await test('explain: missing table and non-SELECT statements', async () => {
    const db = seed();
    const missing = db.explain('SELECT * FROM nope WHERE id = 1');
    assert.strictEqual(missing.statement, 'select');
    assert.strictEqual(missing.table, 'nope');
    assert.strictEqual(missing.tableRows, undefined);

    const ins = db.explain("INSERT INTO users VALUES (201, 'Zed', 40)");
    assert.deepStrictEqual(ins, { statement: 'insert', strategy: 'n/a', hasOrderBy: false });

    const semicolon = db.explain("SELECT * FROM users WHERE name = 'Ann';");
    assert.strictEqual(semicolon.strategy, 'index-scan', 'trailing semicolon tolerated');
    assert.throws(
      () => YASD.parse("SELECT * FROM users; SELECT * FROM users"),
      /trailing token/
    );
    assert.throws(
      () => db.query("SELECT * FROM users;;"),
      /trailing token/
    );
    assert.throws(
      () => db.query("SELECT * FROM users garbage"),
      /trailing token/
    );
    db.close();
  });

  await test('parser hardens quotes, booleans, and numeric literals', async () => {
    const values = YASD.parse("INSERT INTO t VALUES (nUlL, TrUe, FaLsE, 'it''s')").values[0];
    assert.deepStrictEqual(values, [null, true, false, "it's"]);
    const escaped = YASD.parse(String.raw`INSERT INTO t VALUES ('it\'s', 'line\n\t\\')`).values[0];
    assert.deepStrictEqual(escaped, ["it's", 'line\n\t\\']);

    assert.throws(() => YASD.parse("INSERT INTO t VALUES ('unterminated)"), /Unterminated quoted string/);
    assert.throws(() => YASD.parse(String.raw`INSERT INTO t VALUES ('bad\q')`), /Unsupported escape/);
    assert.throws(() => YASD.parse('INSERT INTO t VALUES (10oops)'), /Malformed numeric/);
    assert.throws(() => YASD.parse('INSERT INTO t VALUES (1.2.3)'), /Malformed numeric/);
    assert.throws(() => YASD.parse('INSERT INTO t VALUES (1e+)'), /Malformed numeric/);
    assert.throws(() => YASD.parse('SELECT * FROM t LIMIT 1.5'), /LIMIT must be an integer/);
  });

  await test('parser rejects unknown types and invalid schemas', async () => {
    assert.throws(
      () => YASD.parse('CREATE TABLE unknown_type (id mystery)'),
      /Unknown column type/
    );
    assert.throws(
      () => YASD.parse('CREATE TABLE empty_table ()'),
      /at least one column/
    );
    assert.throws(
      () => YASD.parse('CREATE TABLE duplicate_columns (id int, id string)'),
      /Duplicate column/
    );
    assert.throws(
      () => YASD.parse('CREATE TABLE missing_primary_key (id int, PRIMARY KEY (missing))'),
      /Primary key column.*does not exist/
    );
    assert.throws(
      () => YASD.parse('CREATE TABLE multiple_primary_keys (id int primary key, name string primary key)'),
      /Multiple primary key declarations/
    );
    assert.throws(
      () => YASD.parse('CREATE TABLE multiple_table_primary_keys (id int, name string, PRIMARY KEY (id), PRIMARY KEY (name))'),
      /Multiple primary key declarations/
    );

    assert.doesNotThrow(() => YASD.parse('CREATE TABLE without_primary_key (id int)'));
  });

  await test('executor enforces constraints on INSERT and UPDATE', async () => {
    const db = new YASD({ sweepIntervalMs: 0 })
    db.query('CREATE TABLE constrained (id int primary key, name string NOT NULL, enabled boolean, score number)')
    db.query("INSERT INTO constrained VALUES (1, 'A', true, 10)")

    assert.throws(
      () => db.query("INSERT INTO constrained VALUES (2, 'B', true)"),
      /expected 4/
    )
    assert.throws(
      () => db.query("INSERT INTO constrained VALUES (2, 'B', true, 20, 30)"),
      /expected 4/
    )
    assert.throws(
      () => db.query("INSERT INTO constrained (id, id, name, enabled, score) VALUES (2, 2, 'B', true, 20)"),
      /Duplicate INSERT column/
    )
    assert.throws(
      () => db.query('INSERT INTO constrained VALUES (2, NULL, true, 20)'),
      /cannot be null/
    )
    assert.throws(
      () => db.query("INSERT INTO constrained VALUES (2, 'B', true, 'not-a-number')"),
      /Invalid number value/
    )
    assert.throws(
      () => db.query("INSERT INTO constrained VALUES (2, 'B', 'not-a-boolean', 20)"),
      /Invalid boolean value/
    )
    assert.throws(
      () => db.query("INSERT INTO constrained VALUES (1, 'B', true, 20)"),
      /Duplicate primary key value/
    )

    assert.strictEqual(db.query('SELECT * FROM constrained').rows.length, 1)
    assert.throws(
      () => db.query("INSERT INTO constrained VALUES (2, 'B', true, 20), (1, 'C', true, 30)"),
      /Duplicate primary key value/
    )
    assert.strictEqual(db.query('SELECT * FROM constrained WHERE id = 2').rows.length, 0)

    db.query("INSERT INTO constrained VALUES (2, 'B', true, 20)")

    assert.throws(
      () => db.query('UPDATE constrained SET id = 9 WHERE id >= 1'),
      /Duplicate primary key value/
    )
    assert.strictEqual(db.query('SELECT * FROM constrained WHERE id = 1').rows.length, 1)
    assert.strictEqual(db.query('SELECT * FROM constrained WHERE id = 2').rows.length, 1)
    assert.throws(
      () => db.query('UPDATE constrained SET id = 1 WHERE id = 2'),
      /Duplicate primary key value/
    )
    assert.throws(
      () => db.query("UPDATE constrained SET score = 'not-a-number' WHERE id = 2"),
      /Invalid number value/
    )
    assert.throws(
      () => db.query('UPDATE constrained SET name = NULL WHERE id = 2'),
      /cannot be null/
    )
    assert.strictEqual(db.query('SELECT * FROM constrained WHERE id = 2').rows.length, 1)

    db.query('UPDATE constrained SET id = 3 WHERE id = 2')
    assert.strictEqual(db.query('SELECT * FROM constrained WHERE id = 2').rows.length, 0)
    assert.strictEqual(db.query('SELECT * FROM constrained WHERE id = 3').rows.length, 1)
    db.close()
  })

  await test('NULL predicates follow SQL three-valued logic', async () => {
    const db = new YASD({ sweepIntervalMs: 0 })
    db.query('CREATE TABLE nullable_values (id int, value string)')
    db.query("INSERT INTO nullable_values VALUES (1, NULL), (2, 'x')")

    assert.strictEqual(db.query('SELECT * FROM nullable_values WHERE value = NULL').rows.length, 0)
    assert.strictEqual(db.query('SELECT * FROM nullable_values WHERE value != NULL').rows.length, 0)
    assert.strictEqual(db.query('SELECT * FROM nullable_values WHERE NOT value = NULL').rows.length, 0)
    assert.deepStrictEqual(
      db.query('SELECT id FROM nullable_values WHERE value IS NULL').rows.map(row => row.id),
      [1]
    )
    assert.deepStrictEqual(
      db.query('SELECT id FROM nullable_values WHERE value IS NOT NULL').rows.map(row => row.id),
      [2]
    )
    assert.deepStrictEqual(
      db.query("SELECT id FROM nullable_values WHERE value IN (NULL, 'x')").rows.map(row => row.id),
      [2]
    )
    assert.deepStrictEqual(
      db.query('SELECT id FROM nullable_values WHERE value = NULL OR id = 1').rows.map(row => row.id),
      [1]
    )

    const update = db.query('UPDATE nullable_values SET id = 9 WHERE value != NULL')
    assert.strictEqual(update.affectedRows, 0)
    assert.deepStrictEqual(db.query('SELECT id FROM nullable_values').rows.map(row => row.id), [1, 2])
    db.close()
  })

  await test('referenced columns must exist', async () => {
    const db = seed()
    const assertColumnNotFound = fn => {
      assert.throws(fn, error => error && error.code === 'COLUMN_NOT_FOUND')
    }

    assertColumnNotFound(() => db.query('SELECT missing FROM users'))
    assertColumnNotFound(() => db.query('SELECT * FROM users ORDER BY missing'))
    assertColumnNotFound(() => db.query('SELECT * FROM users WHERE missing = 1'))
    assertColumnNotFound(() => db.explain('SELECT missing FROM users'))
    assertColumnNotFound(() => db.query('UPDATE users SET age = 1 WHERE missing = 1'))
    assertColumnNotFound(() => db.query('DELETE FROM users WHERE missing = 1'))
    db.close()
  })

  await test('ORDER BY keeps NULL last and mixed values deterministic', async () => {
    const db = new YASD({ sweepIntervalMs: 0 })
    db.query('CREATE TABLE order_values (id int, value any)')
    db.query("INSERT INTO order_values VALUES (1, NULL), (2, '10'), (3, 2), (4, '2'), (5, false), (6, true), (7, '10')")

    assert.deepStrictEqual(
      db.query('SELECT id FROM order_values ORDER BY value').rows.map(row => row.id),
      [3, 2, 7, 4, 5, 6, 1]
    )
    assert.deepStrictEqual(
      db.query('SELECT id FROM order_values ORDER BY value DESC').rows.map(row => row.id),
      [6, 5, 4, 2, 7, 3, 1]
    )
    db.close()
  })

  // ---- PROFILE ----

  await test('profile: SELECT reports plan + timing + row counts', async () => {
    const db = seed();
    const sql = "SELECT * FROM users WHERE name = 'Ann' ORDER BY age DESC LIMIT 5";
    const p = db.profile(sql);
    assert.strictEqual(p.statement, 'select');
    assert.strictEqual(p.strategy, 'index-scan');
    assert.deepStrictEqual(p.indexColumns, ['name']);
    assert.deepStrictEqual(p.orderBy, { column: 'age', direction: 'desc' });
    assert.strictEqual(p.limit, 5);
    assert.strictEqual(typeof p.durationMs, 'number');
    assert.ok(p.durationMs >= 0, `duration, got ${p.durationMs}`);
    assert.strictEqual(p.rowsReturned, 5, 'LIMIT applied (40 Ann rows, 5 returned)');
    assert.strictEqual(p.affectedRows, undefined, 'SELECT has no affectedRows');
    // The profiled run matches a direct run (ORDER BY + LIMIT honored).
    const direct = db.query(sql).rows;
    assert.strictEqual(direct.length, p.rowsReturned);
    assert.ok(direct.every(r => r.name === 'Ann'));
    const ages = direct.map(r => r.age);
    assert.deepStrictEqual([...ages].sort((a, b) => b - a), ages, 'descending by age');
    db.close();
  });

  await test('profile: full-scan SELECT and write statements', async () => {
    const db = seed();
    const full = db.profile('SELECT * FROM users WHERE age > 40');
    assert.strictEqual(full.strategy, 'full-scan');
    assert.strictEqual(full.rowsReturned, 54, 'exact count for the seed data');
    assert.strictEqual(
      full.rowsReturned,
      db.query('SELECT * FROM users WHERE age > 40').rows.length,
      'profile count matches direct run'
    );
    assert.ok(full.rowsReturned > 0);
    assert.strictEqual(full.affectedRows, undefined, 'SELECT has no affectedRows');

    const ins = db.profile("INSERT INTO users VALUES (201, 'Zed', 40)");
    assert.strictEqual(ins.statement, 'insert');
    assert.strictEqual(ins.strategy, 'n/a');
    assert.strictEqual(ins.affectedRows, 1);
    assert.strictEqual(ins.rowsReturned, 0);

    const upd = db.profile("UPDATE users SET age = 41 WHERE name = 'Zed'");
    assert.strictEqual(upd.affectedRows, 1);

    const del = db.profile("DELETE FROM users WHERE name = 'Zed'");
    assert.strictEqual(del.affectedRows, 1);
    assert.strictEqual(db.query("SELECT * FROM users WHERE name = 'Zed'").rows.length, 0);
    db.close();
  });

  await test('profile: runs feed the slow-query log when over threshold', async () => {
    const db = seed();
    db.setSlowQueryThreshold(1e-9);
    db.profile("SELECT * FROM users WHERE name = 'Ann'");
    const log = db.slowLog();
    assert.strictEqual(log.length, 1);
    assert.ok(log[0].sql.includes('Ann'), `profiled sql logged, got ${log[0].sql}`);
    db.close();
  });

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(`Profiling tests completed: ${passed + failed}`);
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
