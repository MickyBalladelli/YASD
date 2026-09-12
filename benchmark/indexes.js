#!/usr/bin/env node

const { YASD } = require("../dist/index.js");

const rowCount = Number(process.argv[2] ?? 10000);
const updateCount = Number(process.argv[3] ?? 1000);
if (
  !Number.isSafeInteger(rowCount) ||
  rowCount <= 0 ||
  !Number.isSafeInteger(updateCount) ||
  updateCount <= 0
) {
  throw new Error("usage: node benchmark/indexes.js [rows>0] [updates>0]");
}

function heapMiB() {
  return process.memoryUsage().heapUsed / 1024 / 1024;
}

function run(label, indexColumns) {
  if (typeof global.gc === "function") global.gc();
  const beforeMiB = heapMiB();
  const options = { sweepIntervalMs: 0 };
  if (indexColumns !== undefined) options.indexColumns = indexColumns;
  const db = new YASD(options);

  db.query(
    "CREATE TABLE benchmark_rows (id int primary key, bucket number, payload string)",
  );
  const values = [];
  for (let i = 0; i < rowCount; i++) {
    values.push(`(${i}, ${i % 100}, 'payload-${i}')`);
  }

  const insertStart = process.hrtime.bigint();
  db.query(`INSERT INTO benchmark_rows VALUES ${values.join(",")}`);
  const insertMs = Number(process.hrtime.bigint() - insertStart) / 1e6;

  const updateStart = process.hrtime.bigint();
  for (let i = 0; i < updateCount; i++) {
    const id = i % rowCount;
    db.query(`UPDATE benchmark_rows SET bucket = ${i % 100} WHERE id = ${id}`);
  }
  const updateMs = Number(process.hrtime.bigint() - updateStart) / 1e6;

  const queryStart = process.hrtime.bigint();
  for (let i = 0; i < updateCount; i++) {
    db.query(`SELECT * FROM benchmark_rows WHERE id = ${i % rowCount}`);
  }
  const queryMs = Number(process.hrtime.bigint() - queryStart) / 1e6;

  const afterMiB = heapMiB();
  console.log(
    JSON.stringify({
      label,
      rows: rowCount,
      updates: updateCount,
      heapDeltaMiB: Number((afterMiB - beforeMiB).toFixed(2)),
      insertMs: Number(insertMs.toFixed(2)),
      updateMs: Number(updateMs.toFixed(2)),
      equalityQueryMs: Number(queryMs.toFixed(2)),
      idPlan: db.explain("SELECT * FROM benchmark_rows WHERE id = 1").strategy,
    }),
  );
  db.close();
}

run("all-columns", undefined);
run("id-only", ["id"]);
run("no-indexes", []);
