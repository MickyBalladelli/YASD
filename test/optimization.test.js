const { test } = require("node:test");
const assert = require("node:assert/strict");
const { YASD, KVCache } = require("../dist");
const { Executor } = require("../dist/executor");
const { OrderedMap } = require("../dist/ordered-map");
const { matchLike } = require("../dist/query-utils");

test("P01: nested journals preserve full ordered state and grow only with touched keys", () => {
  const cache = new KVCache({ maxEntries: 10001, sweepIntervalMs: 0 });
  for (let i = 0; i < 10000; i++) cache.set(String(i), { i });
  const before = cache.dump();
  const stats = cache.stats();
  assert.throws(
    () =>
      cache.atomic(() => {
        cache.set("3", "new");
        cache.get("8");
        cache.expire("2", 10000);
        assert.ok(cache.maintenanceStats().undoEntries < 25);
        assert.throws(
          () =>
            cache.atomic(() => {
              cache.clearPrefix("7");
              cache.clear();
              throw Error("inner");
            }),
          /inner/,
        );
        assert.equal(cache.get("3"), "new");
        cache.atomic(() => {
          cache.persist("2");
          cache.del("4");
        });
        throw Error("outer");
      }),
    /outer/,
  );
  assert.deepEqual(cache.dump(), before);
  assert.deepEqual(cache.stats(), stats);
  assert.equal(cache.maintenanceStats().undoEntries, 0);
  cache.close();
});

test("P01: randomized ordered-map undo matches native Map at every savepoint", () => {
  let seed = 20260912;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  const map = new OrderedMap();
  let oracle = new Map();
  for (let run = 0; run < 300; run++) {
    const saved = new Map(oracle);
    map.begin();
    for (let i = 0; i < 40; i++) {
      const key = random() % 80;
      if (random() % 3) {
        const value = random();
        map.set(key, value);
        oracle.set(key, value);
      } else {
        map.delete(key);
        oracle.delete(key);
      }
    }
    if (random() % 2) {
      map.rollback();
      oracle = saved;
    } else map.commit();
    assert.deepEqual([...map], [...oracle], `seed 20260912 run ${run}`);
  }
});

test("S04: bounded expiry visits no persistent keys and preserves exact accounting", () => {
  const cache = new KVCache({ maxEntries: 2000, sweepIntervalMs: 0 });
  const real = Date.now;
  let now = real();
  Date.now = () => now;
  try {
    for (let i = 0; i < 1000; i++) cache.set(`p${i}`, i);
    for (let i = 0; i < 50; i++) cache.set(`e${i}`, i, 10);
    now += 20;
    assert.equal(cache.sweepBudget(7, 1000), 7);
    assert.equal(cache.maintenanceStats().expiringKeys, 43);
    for (let i = 0; i < 20; i++) cache.sweepBudget(7, 1000);
    assert.equal(cache.size, 1000);
    assert.equal(cache.stats().expiries, 50);
    assert.equal(cache.maintenanceStats().maxObservedExpiryLagMs, 10);
  } finally {
    Date.now = real;
    cache.close();
  }
});

test("P03: PK index is independent of query indexes, canonical, and batch-atomic", () => {
  const db = new Executor({ indexColumns: [] });
  db.query("CREATE TABLE t (id ANY PRIMARY KEY, name STRING)");
  db.executeStatement({
    type: "insert",
    tableName: "t",
    columns: [],
    values: [[{ a: 1, b: 2 }, "x"]],
  });
  assert.throws(
    () =>
      db.executeStatement({
        type: "insert",
        tableName: "t",
        columns: [],
        values: [[{ b: 2, a: 1 }, "y"]],
      }),
    (e) => e.code === "PRIMARY_KEY_CONSTRAINT",
  );
  db.query("CREATE TABLE u (id NUMBER PRIMARY KEY, x NUMBER)");
  db.query("INSERT INTO u VALUES (1, 2), (2, 3)");
  assert.throws(
    () => db.query("INSERT INTO u VALUES (3, 4), (3, 5)"),
    (e) => e.code === "PRIMARY_KEY_CONSTRAINT",
  );
  assert.throws(
    () => db.query("UPDATE u SET id = 9"),
    (e) => e.code === "PRIMARY_KEY_CONSTRAINT",
  );
  db.query("UPDATE u SET x = 8");
  db.query("DELETE FROM u WHERE id = 1");
  db.query("INSERT INTO u VALUES (1, 7)");
  assert.deepEqual(db.query("SELECT * FROM u").rows, [
    { id: 2, x: 8 },
    { id: 1, x: 7 },
  ]);
});

test("P04: randomized stable-row indexed writes agree with scan storage", () => {
  const indexed = new Executor();
  const scan = new Executor({ indexColumns: [] });
  let seed = 84712;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  for (const db of [indexed, scan])
    db.query("CREATE TABLE t (id NUMBER PRIMARY KEY, x NUMBER)");
  for (let i = 0; i < 600; i++) {
    const id = random() % 100;
    const op = random() % 3;
    const sql =
      op === 0
        ? `DELETE FROM t WHERE id = ${id}`
        : op === 1
          ? `UPDATE t SET x = ${i % 9} WHERE id = ${id}`
          : `INSERT INTO t VALUES (${id}, ${i % 9})`;
    for (const db of [indexed, scan]) {
      try {
        db.query(sql);
      } catch (e) {
        assert.equal(e.code, "PRIMARY_KEY_CONSTRAINT");
      }
    }
    assert.deepEqual(
      indexed.query("SELECT * FROM t WHERE x IN (1, 3, 7)").rows,
      scan.query("SELECT * FROM t WHERE x IN (1, 3, 7)").rows,
    );
  }
  assert.deepEqual(indexed.stats().rows, scan.stats().rows);
});

test("P05: shared residual plan, early limit, bounded top-k and immutable prepared plans", () => {
  const db = new Executor();
  db.query("CREATE TABLE t (id NUMBER, x NUMBER, name STRING)");
  for (let i = 0; i < 300; i++)
    db.query(`INSERT INTO t VALUES (${i}, ${i % 7}, 'a${i}')`);
  const sql = "SELECT id FROM t WHERE x = 2 AND id > 10 LIMIT 2";
  const plan = db.explain(sql);
  assert.equal(plan.strategy, "index-scan");
  assert.deepEqual(plan.indexColumns, ["x"]);
  plan.columns.push("nope");
  assert.equal(db.query(sql).rows.length, 2);
  assert.ok(db.stats().lastRowsExamined < 10);
  for (const order of ["ASC", "DESC"]) {
    const full = db.query(`SELECT * FROM t ORDER BY x ${order}`).rows;
    assert.deepEqual(
      db.query(`SELECT * FROM t ORDER BY x ${order} LIMIT 11 OFFSET 7`).rows,
      full.slice(7, 18),
    );
  }
  db.query("SELECT * FROM t LIMIT 0");
  assert.equal(db.stats().lastRowsExamined, 0);
  assert.equal(matchLike("a-b-C", "A%_c"), true);
  assert.throws(
    () => matchLike("a".repeat(5000), "%" + "a".repeat(2000) + "b"),
    (e) => e.code === "LIMIT_EXCEEDED",
  );
});

test("S06: SQL aggregate row/byte/result budgets reject atomically and release on delete/reset", () => {
  const db = new YASD({
    sweepIntervalMs: 0,
    sql: { maxRows: 3, maxBytes: 100, maxResultRows: 2, maxResultBytes: 60 },
  });
  try {
    db.query("CREATE TABLE t (id NUMBER PRIMARY KEY, name STRING)");
    db.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    assert.throws(
      () => db.query("INSERT INTO t VALUES (4, 'd')"),
      (e) => e.code === "LIMIT_EXCEEDED",
    );
    assert.throws(
      () => db.query("SELECT * FROM t"),
      (e) => e.code === "LIMIT_EXCEEDED",
    );
    assert.throws(
      () => db.query(`UPDATE t SET name = '${"x".repeat(100)}' WHERE id = 1`),
      (e) => e.code === "LIMIT_EXCEEDED",
    );
    assert.equal(db.query("SELECT * FROM t WHERE id = 1").rows[0].name, "a");
    db.query("DELETE FROM t WHERE id = 2");
    db.query("INSERT INTO t VALUES (4, 'd')");
    db.reset();
    db.query("CREATE TABLE t (id NUMBER)");
    db.query("INSERT INTO t VALUES (1), (2)");
  } finally {
    db.close();
  }
});
