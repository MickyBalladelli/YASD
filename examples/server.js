// Example: shared YASD cache server + Echo adapter sketch.
// Run:  npm run build && node examples/server.js
// Env:  YASD_PORT, CACHE_URL (default yasd://127.0.0.1:7379)

const { YasdServer, YasdClient } = require('../dist/index.js');

const PORT = Number(process.env.YASD_PORT || '7379');

async function main() {
  // 1. Start the standalone cache server (one per deployment, or per dev box).
  const server = new YasdServer({
    host: '127.0.0.1',
    port: PORT,
    snapshotPath: process.env.YASD_SNAPSHOT, // e.g. ./data/snapshot.json
    aofPath: process.env.YASD_AOF, // e.g. ./data/appendonly.aof
    autoSaveMs: 60_000,
  });
  await server.start();
  console.log(`yasd server on 127.0.0.1:${PORT} (health: /healthz)`);

  // 2. Echo-side drop-in adapter on the shared client.
  // Mirrors Echo/server/src/cache/memory.js seam:
  //   cacheKey(ns, v) / cacheGet(key) / cacheSet(key, value, ttlMs) / cacheClear(ns)
  const client = new YasdClient({ url: process.env.CACHE_URL || `yasd://127.0.0.1:${PORT}?poolSize=4` });
  await client.connect();

  const cacheKey = (namespace, value) => `${namespace}:${value}`;
  const cacheGet = key => client.get(key);
  const cacheSet = (key, value, ttlMs) => client.set(key, value, ttlMs);
  const cacheClear = namespace => client.clearPrefix(namespace);

  // Hot feed through the shared cache (all Echo replicas see the same data).
  const key = cacheKey('feeds', 'home');
  await cacheSet(key, { posts: [{ id: 1, text: 'hello' }] }, 15_000);
  console.log('feed:', await cacheGet(key));

  // Rate limiting with an atomic counter + cross-replica invalidation fanout.
  const n = await client.incr('ratelimit:post:alice');
  console.log('alice post count:', n);
  await client.expire('ratelimit:post:alice', 60_000);
  const stop = await client.subscribe('__yasd__:invalidate', (ch, msg) => {
    console.log('invalidate event:', msg);
  });
  await cacheClear('feeds');

  // Healthcheck + graceful shutdown.
  console.log('health:', await client.healthcheck().then(h => h.status));

  const shutdown = async () => {
    await stop();
    await client.close();
    await server.close();
    console.log('bye');
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
