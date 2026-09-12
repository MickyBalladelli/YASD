const { test } = require("node:test");
const assert = require("node:assert/strict");
const { YasdServer, YasdClient } = require("../dist");
const {
  ProductionPostCache,
  CACHE_PREFIX,
  CACHE_KEY_VERSION,
  decodeEntry,
} = require("../examples/production-integration");
const quiet = { warn() {}, error() {} };
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function setup(t, durableStore) {
  const server = new YasdServer({ port: 0 });
  await server.start();
  const client = new YasdClient({
    host: "127.0.0.1",
    port: server.address().port,
    poolSize: 1,
  });
  const cache = new ProductionPostCache({
    client,
    durableStore,
    logger: quiet,
    maxLocalEntries: 2,
  });
  await cache.start();
  t.after(async () => {
    await cache.stop();
    await client.close();
    await server.close();
  });
  return { cache, client, server };
}

test("production cache preserves absolute age, rejects future/over-age entries and bounds L1", async (t) => {
  const { cache, client } = await setup(t, {
    getPost: async () => {
      throw Error("source unavailable");
    },
    savePost: async (_, post) => post,
    deletePost: async () => true,
  });
  const old = {
    cacheKeyVersion: CACHE_KEY_VERSION,
    cachedAt: Date.now() - 29_000,
    value: { old: true },
  };
  assert.equal(decodeEntry(old).cachedAt, old.cachedAt);
  assert.equal(
    decodeEntry({ ...old, cachedAt: Date.now() + 60_000 }),
    undefined,
  );
  assert.equal(
    decodeEntry({ ...old, cachedAt: Date.now() - 60_001 }),
    undefined,
  );
  await client.set(`${CACHE_PREFIX}1`, old);
  await wait(5);
  assert.deepEqual(await cache.getPost(1), { old: true });
  assert.equal(cache.local.get(`${CACHE_PREFIX}1`).cachedAt, old.cachedAt);
  await wait(5);
  await assert.rejects(
    cache.getPost(1, { allowStale: false }),
    /source unavailable/,
  );
  for (let i = 0; i < 100; i++)
    cache.remember(String(i), { ...old, cachedAt: Date.now() });
  assert.equal(cache.local.size, 2);
});

test("production cache fences an in-flight missing-key fill after a durable write", async (t) => {
  let release;
  let source = { revision: 1 };
  const { cache, client } = await setup(t, {
    getPost: () =>
      new Promise((resolve) => {
        const old = { ...source };
        release = () => resolve(old);
      }),
    savePost: async (_, post) => {
      source = post;
      return post;
    },
    deletePost: async () => true,
  });
  const read = cache.getPost(1, { allowStale: false });
  while (!release) await wait(1);
  await cache.savePost(1, { revision: 2 });
  release();
  assert.deepEqual(await read, { revision: 1 }); // Overlapping reads may return their earlier source read.
  assert.equal(cache.local.get(`${CACHE_PREFIX}1`), undefined);
  assert.equal((await client.get(`${CACHE_PREFIX}1`)).deleted, true); // But must never reinstall it.
});

test("production cache clears L1 on subscriber loss and preserves returned-value ownership", async (t) => {
  const { cache, client } = await setup(t, {
    getPost: async () => ({ revision: 1 }),
    savePost: async (_, post) => post,
    deletePost: async () => true,
  });
  cache.remember("local", {
    cacheKeyVersion: CACHE_KEY_VERSION,
    cachedAt: Date.now(),
    value: { revision: 1 },
  });
  const own = cache.local.get("local");
  own.value.revision = 99;
  assert.equal(cache.local.get("local").value.revision, 1);
  client.subSocket.destroy();
  for (let i = 0; i < 100 && cache.local.size; i++) await wait(2);
  assert.equal(cache.local.size, 0);
});
