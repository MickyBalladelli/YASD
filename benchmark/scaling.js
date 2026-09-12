// Reproducible complexity evidence. Not a production capacity certification.
const { performance, monitorEventLoopDelay } = require("node:perf_hooks");
const os = require("node:os");
const { Executor } = require("../dist/executor");
const { KVCache, RespDecoder, encodeReply } = require("../dist");
const assert = require("node:assert/strict");
const median = (values) =>
  [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const timed = (fn) => {
  const start = performance.now();
  fn();
  return performance.now() - start;
};
const rows = [];
for (const n of [1000, 10000, 100000]) {
  global.gc?.();
  const before = process.memoryUsage();
  const db = new Executor();
  db.query("CREATE TABLE t (id NUMBER PRIMARY KEY, n NUMBER)");
  const insertMs = timed(() =>
    db.executeStatement({
      type: "insert",
      tableName: "t",
      columns: [],
      values: Array.from({ length: n }, (_, i) => [i, i % 17]),
    }),
  );
  const updateMs = [];
  const deleteMs = [];
  const topKMs = [];
  for (let run = 0; run < 5; run++) {
    updateMs.push(
      timed(() => {
        for (let i = 0; i < 100; i++)
          db.query(`UPDATE t SET n = 9 WHERE id = ${i}`);
      }),
    );
    topKMs.push(
      timed(() => db.query("SELECT * FROM t ORDER BY n DESC LIMIT 10")),
    );
    deleteMs.push(
      timed(() => db.query(`DELETE FROM t WHERE id = ${n - 1 - run}`)),
    );
  }
  const after = process.memoryUsage();
  const cache = new KVCache({ maxEntries: n + 1, sweepIntervalMs: 0 });
  for (let i = 0; i < n; i++)
    cache.set(String(i), { i, text: "small realistic object" });
  let maxJournal = 0;
  const writes = [];
  for (let run = 0; run < 5; run++)
    writes.push(
      timed(() => {
        for (let i = 0; i < 1000; i++)
          cache.atomic(() => {
            cache.set("0", i);
            maxJournal = Math.max(
              maxJournal,
              cache.maintenanceStats().undoEntries,
            );
          });
      }),
    );
  assert.ok(maxJournal <= 5, `undo work grows with resident size at ${n}`);
  cache.close();
  rows.push({
    rows: n,
    insertMs,
    update100MedianMs: median(updateMs),
    selectiveDeleteMedianMs: median(deleteMs),
    top10MedianMs: median(topKMs),
    atomic1000MedianMs: median(writes),
    maxJournal,
    sqlHeapDelta: after.heapUsed - before.heapUsed,
    sqlRssDelta: after.rss - before.rss,
  });
}
const fragments = [];
for (const n of [262144, 524288, 1048576, 2097152]) {
  const bytes = encodeReply({ kind: "bulk", value: "x".repeat(n) });
  const times = [];
  for (let run = 0; run < 6; run++) {
    const decoder = new RespDecoder();
    const ms = timed(() => {
      for (let i = 0; i < bytes.length; i += 256)
        decoder.push(bytes.subarray(i, i + 256));
    });
    if (run) times.push(ms);
  }
  fragments.push({ payloadBytes: n, chunkBytes: 256, medianMs: median(times) });
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      cpu: os.cpus()[0]?.model,
      seed: "sequential IDs, modulo 17",
      repeats: 5,
      note: "single-process core scaling, excludes SQL parse from initial bulk insert; allocator/GC noise remains",
      rows,
      fragments,
      processMaxRssKiB: process.resourceUsage().maxRSS,
    },
    null,
    2,
  ),
);
