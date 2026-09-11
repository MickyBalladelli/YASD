#!/usr/bin/env node
// Concurrent load benchmark for cache, SQL, pub/sub, client pooling, and
// persistence. Reports latency percentiles and process memory, not only ops.
// Run: node --expose-gc benchmark/load.js [operations] [concurrency] [rows]

let mod
try {
  mod = require('../dist/index.js')
} catch {
  console.error('YASD build missing: run npm run build before benchmarking')
  process.exit(1)
}

const fs = require('fs')
const os = require('os')
const path = require('path')
const {
  AofLog,
  PubSubHub,
  YasdClient,
  YasdServer,
  YASD,
  saveSnapshot,
} = mod

const MAX_OPERATIONS = 1_000_000
const MAX_CONCURRENCY = 1024
const MAX_ROWS = 1_000_000

function positiveInteger(raw, name, fallback, maximum) {
  const value = raw === undefined ? fallback : Number(raw)
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new Error(`${name} must be a safe integer from 1 to ${maximum}`)
  }
  return value
}

const [rawOperations, rawConcurrency, rawRows] = process.argv.slice(2)
const operations = positiveInteger(rawOperations, 'operations', 5000, MAX_OPERATIONS)
const concurrency = positiveInteger(rawConcurrency, 'concurrency', 4, MAX_CONCURRENCY)
const rows = positiveInteger(rawRows, 'rows', 5000, MAX_ROWS)

function nowNs() {
  return process.hrtime.bigint()
}

function elapsedMs(start) {
  return Number(nowNs() - start) / 1e6
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function memory() {
  const usage = process.memoryUsage()
  return { heapUsed: usage.heapUsed, rss: usage.rss }
}

function mib(bytes) {
  return bytes / 1024 / 1024
}

function largerMemory(left, right) {
  return {
    heapUsed: Math.max(left.heapUsed, right.heapUsed),
    rss: Math.max(left.rss, right.rss),
  }
}

function percentile(values, fraction) {
  const sorted = Array.from(values).sort((a, b) => a - b)
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1))
  return sorted[index]
}

function maximum(values) {
  let result = 0
  for (const value of values) result = Math.max(result, value)
  return result
}

function rounded(value) {
  return Number(value.toFixed(3))
}

async function runWorkload(label, count, workers, operation, details = {}) {
  if (typeof global.gc === 'function') global.gc()
  const latencies = new Float64Array(count)
  const before = memory()
  let peak = before
  let next = 0
  const started = nowNs()

  async function worker(workerIndex) {
    for (;;) {
      const index = next++
      if (index >= count) return
      const operationStarted = nowNs()
      await operation(index, workerIndex)
      latencies[index] = elapsedMs(operationStarted)
      if ((index & 127) === 0) peak = largerMemory(peak, memory())
    }
  }

  await Promise.all(Array.from(
    { length: Math.min(workers, count) },
    (_, workerIndex) => worker(workerIndex)
  ))
  peak = largerMemory(peak, memory())
  const after = memory()
  const totalMs = Number(nowNs() - started) / 1e6

  return {
    label,
    operations: count,
    concurrency: Math.min(workers, count),
    elapsedMs: rounded(totalMs),
    opsPerSecond: rounded(count / (totalMs / 1000)),
    p50Ms: rounded(percentile(latencies, 0.5)),
    p95Ms: rounded(percentile(latencies, 0.95)),
    p99Ms: rounded(percentile(latencies, 0.99)),
    maxMs: rounded(maximum(latencies)),
    heapBeforeMiB: rounded(mib(before.heapUsed)),
    heapAfterMiB: rounded(mib(after.heapUsed)),
    heapDeltaMiB: rounded(mib(after.heapUsed - before.heapUsed)),
    heapPeakDeltaMiB: rounded(mib(peak.heapUsed - before.heapUsed)),
    rssBeforeMiB: rounded(mib(before.rss)),
    rssAfterMiB: rounded(mib(after.rss)),
    rssDeltaMiB: rounded(mib(after.rss - before.rss)),
    rssPeakDeltaMiB: rounded(mib(peak.rss - before.rss)),
    ...details,
  }
}

function seedRows(db, table, rowCount) {
  db.query(`CREATE TABLE ${table} (id int primary key, bucket number, payload string)`)
  const batchSize = 500
  for (let start = 0; start < rowCount; start += batchSize) {
    const values = []
    const end = Math.min(rowCount, start + batchSize)
    for (let i = start; i < end; i++) {
      values.push(`(${i}, ${i % 100}, 'payload-${i}')`)
    }
    db.query(`INSERT INTO ${table} VALUES ${values.join(',')}`)
  }
}

async function benchmarkLru() {
  const capacity = Math.max(1, Math.floor(rows * 0.75))
  const db = new YASD({ maxEntries: capacity, sweepIntervalMs: 0 })
  for (let i = 0; i < rows; i++) db.set(`lru:${i}`, i)
  try {
    return await runWorkload('lru-get-set', operations, concurrency, async i => {
      const key = `lru:${i % rows}`
      if (i % 5 === 0) db.set(key, i)
      else db.get(key)
    }, { maxEntries: capacity })
  } finally {
    db.close()
  }
}

async function benchmarkExpiry() {
  const db = new YASD({ maxEntries: rows * 2, sweepIntervalMs: 5 })
  for (let i = 0; i < rows; i++) db.set(`expiry:${i}`, i, 1)
  await delay(10)
  try {
    return await runWorkload('expiry-sweeping', operations, concurrency, async i => {
      db.set(`expiry:${i % rows}`, i, 5)
      if ((i & 3) === 0) db.cacheSweep()
    }, { sweepIntervalMs: 5 })
  } finally {
    db.close()
  }
}

async function benchmarkIndexedReads() {
  const db = new YASD({ sweepIntervalMs: 0, indexColumns: ['id'] })
  seedRows(db, 'indexed_rows', rows)
  try {
    return await runWorkload('indexed-reads', operations, concurrency, i => {
      db.query(`SELECT * FROM indexed_rows WHERE id = ${i % rows}`)
    }, { plan: db.explain('SELECT * FROM indexed_rows WHERE id = 1').strategy })
  } finally {
    db.close()
  }
}

async function benchmarkFullScanReads() {
  const db = new YASD({ sweepIntervalMs: 0, indexColumns: [] })
  seedRows(db, 'scan_rows', rows)
  try {
    return await runWorkload('full-scan-reads', operations, concurrency, i => {
      db.query(`SELECT * FROM scan_rows WHERE id = ${i % rows}`)
    }, { plan: db.explain('SELECT * FROM scan_rows WHERE id = 1').strategy })
  } finally {
    db.close()
  }
}

async function benchmarkUpdates() {
  const db = new YASD({ sweepIntervalMs: 0, indexColumns: ['id'] })
  seedRows(db, 'update_rows', rows)
  try {
    return await runWorkload('indexed-updates', operations, concurrency, i => {
      db.query(`UPDATE update_rows SET bucket = ${i % 100} WHERE id = ${i % rows}`)
    })
  } finally {
    db.close()
  }
}

async function benchmarkDeletes() {
  const db = new YASD({ sweepIntervalMs: 0, indexColumns: ['id'] })
  seedRows(db, 'delete_rows', rows)
  const count = Math.min(operations, rows)
  try {
    return await runWorkload('indexed-deletes', count, concurrency, i => {
      db.query(`DELETE FROM delete_rows WHERE id = ${i}`)
    })
  } finally {
    db.close()
  }
}

async function benchmarkPubSub() {
  const hub = new PubSubHub()
  const subscriberCount = Math.min(1000, Math.max(16, concurrency * 16))
  for (let i = 0; i < subscriberCount; i++) hub.subscribe('fanout', () => undefined)
  try {
    return await runWorkload('pubsub-fanout', operations, concurrency, i => {
      hub.publish('fanout', `message-${i}`)
    }, { subscribers: subscriberCount })
  } finally {
    hub.unsubscribeAll()
  }
}

async function benchmarkPooledClient() {
  const server = new YasdServer({
    host: '127.0.0.1',
    port: 0,
    cache: { sweepIntervalMs: 0 },
    saveOnShutdown: false,
  })
  await server.start()
  const client = new YasdClient({
    host: '127.0.0.1',
    port: server.address().port,
    poolSize: concurrency,
  })
  await client.connect()
  for (let i = 0; i < Math.min(rows, concurrency * 4); i++) {
    await client.set(`pool:${i}`, i)
  }
  try {
    return await runWorkload('pooled-client-traffic', operations, concurrency, async i => {
      const key = `pool:${i % rows}`
      if (i % 3 === 0) await client.set(key, i)
      else await client.get(key)
    }, { poolSize: concurrency })
  } finally {
    await client.close()
    await server.close()
  }
}

async function benchmarkPersistence() {
  const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'yasd-bench-'))
  const aofPath = path.join(artifactDir, 'benchmark.aof')
  const snapshotPath = path.join(artifactDir, 'benchmark.snapshot.json')
  const db = new YASD({ sweepIntervalMs: 0 })
  for (let i = 0; i < Math.min(rows, 1000); i++) db.set(`snapshot:${i}`, { value: i })
  const log = new AofLog(aofPath)
  try {
    const aof = await runWorkload('persistence-aof-append', operations, concurrency, i => {
      log.append({ op: 'set', key: `aof:${i}`, value: { value: i } })
    }, { artifact: aofPath })
    const snapshotCount = Math.min(100, operations)
    const snapshots = await runWorkload(
      'persistence-snapshot-save',
      snapshotCount,
      Math.min(concurrency, 4),
      () => saveSnapshot(db, snapshotPath),
      { artifact: snapshotPath }
    )
    return [
      aof,
      snapshots,
      {
        label: 'persistence-artifacts',
        aofBytes: fs.statSync(aofPath).size,
        snapshotBytes: fs.statSync(snapshotPath).size,
        artifactDir,
      },
    ]
  } finally {
    db.close()
  }
}

async function main() {
  const results = []
  results.push(await benchmarkLru())
  results.push(await benchmarkExpiry())
  results.push(await benchmarkIndexedReads())
  results.push(await benchmarkFullScanReads())
  results.push(await benchmarkUpdates())
  results.push(await benchmarkDeletes())
  results.push(await benchmarkPubSub())
  results.push(await benchmarkPooledClient())
  results.push(...await benchmarkPersistence())
  console.log(JSON.stringify({
    node: process.version,
    operations,
    concurrency,
    rows,
    forcedGc: typeof global.gc === 'function',
    results,
  }, null, 2))
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
