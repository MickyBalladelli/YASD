const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { YasdServer, YasdClient, AofLog, KVCache, loadSnapshot } = require('../dist');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

test('runtime LOAD survives restart as an atomic AOF replacement', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'yasd-load-'));
  const options = { port: 0, snapshotPath: path.join(dir, 'snapshot.json'), aofPath: path.join(dir, 'cache.aof'), saveOnShutdown: false };
  let server = new YasdServer(options);
  try {
    await server.start(); server.cache.set('saved', 1); await server.save();
    server.cache.set('later', 2); await server.load(); assert.equal(server.cache.get('later'), undefined);
    await server.close(); server = new YasdServer(options); await server.start();
    assert.equal(server.cache.get('saved'), 1); assert.equal(server.cache.get('later'), undefined);
  } finally { await server.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test('failed EXEC writes neither AOF nor invalidation, and restores all values', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'yasd-exec-'));
  const aofPath = path.join(dir, 'cache.aof');
  const server = new YasdServer({ port: 0, aofPath }); let client;
  try {
    await server.start(); client = new YasdClient({ host: '127.0.0.1', port: server.address().port, poolSize: 1 });
    server.cache.set('not-number', 'text'); const before = fs.readFileSync(aofPath, 'utf8');
    const events = []; const unsubscribe = server.pubsub.subscribe('__yasd__:invalidate', (_, event) => events.push(event));
    const tx = client.multi(); await tx.set('first', 1); await tx.incr('not-number');
    await assert.rejects(tx.exec());
    assert.equal(server.cache.get('first'), undefined); assert.equal(server.cache.get('not-number'), 'text');
    assert.equal(fs.readFileSync(aofPath, 'utf8'), before); assert.deepEqual(events, []);
    unsubscribe();
  } finally { if (client) await client.close(); await server.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test('persistence rejects invalid UTF-8 and oversized files without replacing state', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'yasd-invalid-'));
  const cache = new KVCache({ sweepIntervalMs: 0 }); cache.set('before', 1);
  try {
    const bad = path.join(dir, 'bad.aof');
    fs.writeFileSync(bad, Buffer.concat([Buffer.from('{"op":"set","key":"x","value":"'), Buffer.from([0xff]), Buffer.from('"}\n')]));
    assert.throws(() => new AofLog(bad), /UTF-8/);
    const big = path.join(dir, 'big.json'); const fd = fs.openSync(big, 'w'); fs.ftruncateSync(fd, 257 * 1024 * 1024); fs.closeSync(fd);
    await assert.rejects(loadSnapshot(cache, big), /256 MiB/);
    assert.equal(cache.get('before'), 1);
  } finally { cache.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test('largest cache values remain readable; oversized MGET/EXEC fail without committing', async () => {
  const server = new YasdServer({ port: 0 }); let client;
  try {
    await server.start(); client = new YasdClient({ host: '127.0.0.1', port: server.address().port, poolSize: 1 });
    const value = 'x'.repeat(4 * 1024 * 1024 - 2);
    await client.set('big', value); assert.equal((await client.get('big')).length, value.length);
    await assert.rejects(client.mget(['big', 'big']), error => error.code === 'LIMIT_EXCEEDED');
    const tx = client.multi(); await tx.set('first', 1);
    // Raw queued MGETs exercise the server output budget; transaction client only exposes immediate reads.
    await tx.queue(['MGET', 'big']); await tx.queue(['MGET', 'big']);
    await assert.rejects(tx.exec()); assert.equal(server.cache.get('first'), undefined);
    assert.equal(await client.ping(), 'PONG');
  } finally { if (client) await client.close(); await server.close(); }
});

test('remote SAVE/LOAD cannot select arbitrary filesystem paths', async () => {
  const server = new YasdServer({ port: 0 }); let client;
  try {
    await server.start(); client = new YasdClient({ host: '127.0.0.1', port: server.address().port, poolSize: 1 });
    await assert.rejects(client.save('/not-allowed/snapshot.json'), error => error.code === 'AUTH_ERROR');
    await assert.rejects(client.load('/not-allowed/snapshot.json'), error => error.code === 'AUTH_ERROR');
    assert.equal(await client.ping(), 'PONG');
  } finally { if (client) await client.close(); await server.close(); }
});

test('autosave coalesces rather than growing an unbounded persistence queue', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'yasd-autosave-'));
  const server = new YasdServer({ port: 0, snapshotPath: path.join(dir, 'snapshot.json'), autoSaveMs: 1, saveOnShutdown: false });
  try {
    await server.start(); server.cache.set('key', 1);
    for (let i = 0; i < 100 && !fs.existsSync(server.snapshotPath); i++) await delay(3);
    assert.ok(fs.existsSync(server.snapshotPath)); assert.ok(server.persistenceDepth <= 1);
  } finally { await server.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});
