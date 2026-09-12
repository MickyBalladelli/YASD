const { test } = require("node:test");
const assert = require("node:assert/strict");
const net = require("node:net");
const { once } = require("node:events");
const {
  YasdClient,
  YasdServer,
  KVCache,
  RespDecoder,
  encodeReply,
  encodeCommand,
} = require("../dist");
const { cloneJsonValue } = require("../dist/json");
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("Z01: early JSON byte budgets match UTF-8 escaped serialization boundaries", () => {
  for (const value of [
    "abc",
    '\u0000\n"\\',
    "\ud800",
    "é🎉",
    { a: [1, true, null, "é"] },
    [],
  ]) {
    const size = Buffer.byteLength(JSON.stringify(value));
    assert.deepEqual(cloneJsonValue(value, "test", size), value);
    assert.throws(
      () => cloneJsonValue(value, "test", size - 1),
      (e) => e.code === "LIMIT_EXCEEDED",
    );
  }
  let touched = false;
  const value = {
    huge: "x".repeat(100000),
    get after() {
      touched = true;
      return 1;
    },
  };
  assert.throws(
    () => cloneJsonValue(value, "test", 20),
    (e) => e.code === "LIMIT_EXCEEDED",
  );
  assert.equal(touched, false);
});

test("P02: resumable decoder accepts single-byte fragmentation and strict UTF-8", () => {
  const value = {
    kind: "array",
    items: [
      { kind: "bulk", value: "é😀".repeat(10000) },
      { kind: "array", items: [{ kind: "int", value: 4 }, { kind: "nil" }] },
      { kind: "bulk", value: "\ufefftext" },
    ],
  };
  const bytes = encodeReply(value);
  const decoder = new RespDecoder();
  const replies = [];
  for (let i = 0; i < bytes.length; i++)
    replies.push(...decoder.push(bytes.subarray(i, i + 1)));
  assert.deepEqual(replies, [value]);
  assert.equal(decoder.bufferedBytes, 0);
  assert.throws(
    () => decoder.push(Buffer.from([36, 49, 13, 10, 255, 13, 10])),
    (e) => e.code === "PROTOCOL_ERROR",
  );
  assert.deepEqual(decoder.push("+OK\r\n"), [{ kind: "simple", value: "OK" }]);
});

test("S04: prefix clearing is available in bounded maintenance batches", () => {
  const cache = new KVCache({ sweepIntervalMs: 0 });
  try {
    for (let i = 0; i < 130; i++) cache.set(`batch:${i}`, i);
    cache.set("other", 1);
    const batches = [...cache.clearPrefixBatches("batch", 7)];
    assert.ok(batches.every((b) => b.scanned <= 7));
    assert.equal(
      batches.reduce((sum, b) => sum + b.removed, 0),
      130,
    );
    assert.equal(cache.size, 1);
    assert.equal(cache.get("other"), 1);
  } finally {
    cache.close();
  }
});

test("S03: dedicated transaction slots release on finish and parent close", async () => {
  const server = new YasdServer({ port: 0 });
  await server.start();
  const client = new YasdClient({
    port: server.address().port,
    poolSize: 1,
    maxTransactions: 2,
  });
  try {
    const a = client.multi();
    const b = client.multi();
    assert.throws(
      () => client.multi(),
      (e) => e.code === "LIMIT_EXCEEDED",
    );
    await a.exec();
    const c = client.multi();
    await b.close();
    await c.discard();
    const stops = [];
    for (let i = 0; i < 64; i++) stops.push(await client.subscribe("bounded", () => {}));
    await assert.rejects(client.subscribe("bounded", () => {}), e => e.code === "LIMIT_EXCEEDED");
    for (const stop of stops) await stop();
    const held = client.multi();
    let resume;
    const gate = new Promise(resolve => { resume = resolve; });
    held.connect = () => gate;
    const key = "x".repeat(2 * 1024 * 1024);
    const pending = [held.get(key), held.get(key), held.get(key)].map(p => p.then(() => false, () => true));
    await assert.rejects(held.get(key), e => e.code === "LIMIT_EXCEEDED");
    await held.close(); resume();
    assert.ok((await Promise.all(pending)).every(Boolean));
    const d = client.multi();
    await d.get("missing");
    await client.close();
    assert.equal(d.finished, true);
  } finally {
    await client.close();
    await server.close();
  }
});

test("S03: total request and AUTH deadlines settle silent TCP peers without late commands", async () => {
  const sockets = new Set();
  const traffic = [];
  const peer = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("data", (b) => traffic.push(b.toString()));
    socket.on("close", () => sockets.delete(socket));
  });
  peer.listen(0, "127.0.0.1");
  await once(peer, "listening");
  const port = peer.address().port;
  const client = new YasdClient({
    port,
    password: "test",
    poolSize: 1,
    connectTimeoutMs: 120,
    requestTimeoutMs: 30,
  });
  const noRequestTimer = new YasdClient({
    port,
    password: "test",
    poolSize: 1,
    connectTimeoutMs: 60,
    requestTimeoutMs: 0,
  });
  try {
    const start = Date.now();
    await assert.rejects(client.set("never", 1), (e) => e.code === "TIMEOUT");
    assert.ok(Date.now() - start < 500);
    await assert.rejects(noRequestTimer.ping(), (e) => e.code === "TIMEOUT");
    const tx = noRequestTimer.multi();
    await assert.rejects(tx.get("x"), (e) => e.code === "TIMEOUT");
    await tx.close();
    await assert.rejects(
      noRequestTimer.subscribe("x", () => {}),
      (e) => e.code === "TIMEOUT",
    );
    const channel = "x".repeat(2 * 1024 * 1024);
    const waiting = Array.from({ length: 3 }, () => noRequestTimer.subscribe(channel, () => {}).catch(e => e));
    await assert.rejects(noRequestTimer.subscribe(channel, () => {}), e => e.code === "LIMIT_EXCEEDED");
    await Promise.all(waiting);
    await delay(150);
    assert.ok(traffic.every((text) => !text.includes("SET")));
  } finally {
    await client.close();
    await noRequestTimer.close();
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => peer.close(resolve));
  }
});

test("S01/S02: aggregate retained-work budget disconnects offender and releases reservations", async () => {
  const server = new YasdServer({ port: 0, maxInflightBytes: 32000 });
  await server.start();
  const client = new YasdClient({ port: server.address().port, poolSize: 1 });
  const socket = net.connect(server.address().port, "127.0.0.1");
  socket.on("error", () => {});
  socket.resume();
  try {
    await once(socket, "connect");
    socket.write(encodeCommand(["MULTI"]));
    for (let i = 0; i < 30; i++)
      socket.write(
        encodeCommand(["SET", `key${i}`, JSON.stringify("x".repeat(2000))]),
      );
    await Promise.race([
      new Promise((resolve) => socket.once("close", resolve)),
      delay(1000).then(() => {
        throw Error("overload was not disconnected");
      }),
    ]);
    assert.ok(server.resourceStats().inflightBytes <= 32000);
    assert.equal(await client.ping(), "PONG");
    assert.equal(server.cache.get("key0"), undefined);
    await client.close();
    for (let i = 0; i < 100 && server.resourceStats().inflightBytes; i++)
      await delay(5);
    assert.equal(server.resourceStats().inflightBytes, 0);
  } finally {
    socket.destroy();
    await client.close();
    await server.close();
  }
});
