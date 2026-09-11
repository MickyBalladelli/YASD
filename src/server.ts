// Standalone YASD cache server: one TCP port serves the RESP-like KV
// protocol and an HTTP `/healthz` endpoint (detected from the first bytes).
// Multi-instance Echo replicas point at it via YasdClient + CACHE_URL.
//
// Wire protocol (RESP2 subset, commands are arrays of bulk strings):
//   PING [msg] | AUTH password | GET key | SET key <json> [PX ms]
//   CAS key <expectedJson|empty=missing> <valueJson> [PX ms]
//   MSET k <json> ... | MGET k ... | DEL k ... | CLEAR prefix
//   TTL key | EXPIRE key ms | PERSIST key | INCR key [by] | DECR key [by]
//   WATCH k ... | UNWATCH | MULTI | EXEC | DISCARD
//   PUBLISH channel msg | SUBSCRIBE ch ... | UNSUBSCRIBE [ch ...]
//   INFO | SAVE [path] | LOAD [path] | QUIT
// Auth: set YASD_PASSWORD (or YASD_REQUIREPASS); clients send AUTH first.
// TLS: set YASD_TLS_KEY + YASD_TLS_CERT; optional YASD_TLS_CA enables
// client-certificate verification when requestCert/rejectUnauthorized are set.
// Cache values travel as JSON bulk strings (objects/arrays supported).
// Mutations are appended to the AOF (when configured) and published as
// invalidation events on `__yasd__:invalidate` for other replicas.
//
// Transactions (multi-key read-modify-write, Redis-style optimistic):
//   WATCH k ...   snapshot versions; EXEC aborts when any watched key changed
//   MULTI         start queueing (only KV ops + PING may be queued)
//   EXEC          commit atomically; array of per-op replies, or nil (*-1)
//                 when a watched key was modified (nothing applied)
//   DISCARD       drop the queue (and the watches) without committing
// EXEC always clears watches, committed or not. DISCARD clears them too.

import * as net from 'net';
import * as tls from 'tls';
import * as fs from 'fs';
import { performance } from 'perf_hooks';
import { SlowLog, SlowEntry, checkSlowThreshold } from './metrics';
import {
  KVCache,
  KVOptions,
  KVBatchEntry,
  KVStats,
  SnapshotEntry,
  validateKVOptions,
} from './cache';
import { PubSubHub, PubSubListener, INVALIDATE_CHANNEL, invalidateMessage, InvalidationEvent } from './pubsub';
import {
  saveSnapshot,
  loadSnapshot,
  AofLog,
  AofOp,
  AofMutation,
  AofBatchEntry,
  SnapshotLoadMetadata,
  AofRecoveryState,
} from './persistence';
import {
  RespDecoder,
  RespReply,
  encodeReply,
  encodeCommand,
  requestArgv,
} from './protocol';
import {
  parsePort,
  parseStrictInteger,
  parseStrictNonNegativeNumber,
  parseStrictNumber,
  validateHost,
  validateNonNegativeNumber,
  validatePort,
  validatePositiveSafeInteger,
} from './validation';

export const DEFAULT_HOST = '127.0.0.1'
export const DEFAULT_PORT = 7379;

/** TLS identity plus optional client-certificate and protocol settings. */
export type YasdServerTlsOptions = tls.TlsOptions & {
  key: string | Buffer;
  cert: string | Buffer;
};

/** Commands that may be queued between MULTI and EXEC (KV ops + PING). */
const TX_QUEUEABLE = new Set([
  'PING',
  'GET',
  'MGET',
  'SET',
  'MSET',
  'DEL',
  'CLEAR',
  'TTL',
  'EXPIRE',
  'PERSIST',
  'INCR',
  'DECR',
  'CAS',
]);

/** Commands whose cache mutation must be covered by AOF persistence. */
const AOF_COMMANDS = new Set([
  'SET',
  'MSET',
  'DEL',
  'CLEAR',
  'EXPIRE',
  'PERSIST',
  'INCR',
  'DECR',
  'CAS',
]);

/** Maximum queued output per connection after socket backpressure. */
const DEFAULT_MAX_PENDING_OUTPUT_BYTES = 1024 * 1024;

export interface YasdServerOptions {
  host?: string;
  port?: number;
  cache?: KVOptions;
  snapshotPath?: string;
  aofPath?: string;
  /** Restore snapshot + replay AOF on start. Default true. */
  loadOnStart?: boolean;
  /** SAVE snapshot on graceful shutdown. Default: true when snapshotPath set. */
  saveOnShutdown?: boolean;
  /** Periodic SAVE interval in ms. Default 0 (off). */
  autoSaveMs?: number;
  /**
   * Log commands slower than this (ms) into the slow-command ring
   * (exposed via INFO). Default 0 (off); fractional values allowed.
   */
  slowCommandMs?: number;
  /** Maximum queued output bytes per connection. Slow consumers are disconnected. Default 1 MiB. */
  maxPendingOutputBytes?: number;
  /**
   * Password for the AUTH command. When set, every command except AUTH/QUIT
   * is rejected with NOAUTH until authenticated. HTTP /healthz stays open
   * (load balancers shouldn't need the secret).
   */
  password?: string;
  /** TLS identity and optional client-certificate settings. */
  tls?: YasdServerTlsOptions;
}

export interface ServerInfo {
  status: 'ok';
  version: string;
  uptimeMs: number;
  connections: number;
  tls: boolean;
  auth: boolean;
  entries: number;
  bytes: number;
  hits: number;
  misses: number;
  evictions: number;
  expiries: number;
  aofEnabled: boolean;
  aofDegraded: boolean;
  aofLastError?: string;
  aofRecoveryState: AofRecoveryState
  aofRecoveryError?: string
  subscribers: number;
  channels: string[];
  /** Slow-command threshold in ms (0 = off). */
  slowCommandMs: number;
  /** Newest-first slow-command ring (capped at 100). */
  slowLog: SlowEntry[];
}

interface ConnState {
  socket: net.Socket;
  decoder: RespDecoder;
  httpBuf: Buffer | null; // non-null once HTTP detected
  subs: Map<string, PubSubListener>; // active subscriptions (empty = normal mode, null = never-subscribed?)
  subMode: boolean;
  authed: boolean;
  /** Watched key versions (WATCH), cleared by EXEC/DISCARD/UNWATCH. */
  watchVersions: Map<string, number> | null;
  /** Queued commands between MULTI and EXEC (null = not in MULTI). */
  txQueue: Array<{ cmd: string; args: string[] }> | null;
  /** User-space output waiting for the socket's drain event. */
  outputQueue: Buffer[];
  outputQueueBytes: number;
  outputBackpressured: boolean;
  closeWhenDrained: boolean;
  outputClosed: boolean;
}

interface TransactionEffects {
  aof: AofMutation[];
  invalidations: InvalidationEvent[];
}

function isLoopbackHost(host: string): boolean {
  const normalized = host.trim().toLowerCase().replace(/^\[|\]$/g, '')
  if (normalized === 'localhost' || normalized === '::1') return true
  if (net.isIP(normalized) !== 4) return false
  const firstOctet = Number.parseInt(normalized.split('.')[0] as string, 10)
  return firstOctet === 127
}

function parseBoolean(value: string, name: string): boolean {
  if (value === '1' || value.toLowerCase() === 'true') return true;
  if (value === '0' || value.toLowerCase() === 'false') return false;
  throw new Error(`${name} must be true/false or 1/0`);
}

function parseTlsVersion(value: string): tls.SecureVersion {
  const allowed = new Set(['TLSv1', 'TLSv1.1', 'TLSv1.2', 'TLSv1.3']);
  if (!allowed.has(value)) throw new Error(`invalid YASD_TLS_MIN_VERSION: ${value}`);
  return value as tls.SecureVersion;
}

export function serverOptionsFromEnv(env: NodeJS.ProcessEnv = process.env): YasdServerOptions {
  const cache: KVOptions = {};
  if (env.CACHE_MAX_ENTRIES !== undefined) {
    cache.maxEntries = parseStrictInteger(env.CACHE_MAX_ENTRIES, 'CACHE_MAX_ENTRIES', 1);
  }
  if (env.CACHE_MAX_BYTES !== undefined) {
    cache.maxBytes = parseStrictInteger(env.CACHE_MAX_BYTES, 'CACHE_MAX_BYTES', 1);
  }
  if (env.CACHE_DEFAULT_TTL_MS !== undefined) {
    cache.defaultTTLMs = parseStrictNonNegativeNumber(env.CACHE_DEFAULT_TTL_MS, 'CACHE_DEFAULT_TTL_MS');
  }
  if (env.CACHE_NAMESPACE_TTLS !== undefined) {
    try {
      cache.namespaceTTLMs = JSON.parse(env.CACHE_NAMESPACE_TTLS) as Record<string, number>;
    } catch {
      throw new Error('CACHE_NAMESPACE_TTLS must be JSON, e.g. {"feeds":15000}');
    }
  }
  validateKVOptions(cache);
  const opts: YasdServerOptions = {
    host: env.YASD_HOST === undefined ? DEFAULT_HOST : validateHost(env.YASD_HOST, 'YASD_HOST'),
    port: env.YASD_PORT === undefined ? DEFAULT_PORT : parsePort(env.YASD_PORT, 'YASD_PORT'),
    cache,
  };
  if (env.YASD_SNAPSHOT) opts.snapshotPath = env.YASD_SNAPSHOT;
  if (env.YASD_AOF) opts.aofPath = env.YASD_AOF;
  if (env.YASD_AUTO_SAVE_MS !== undefined) {
    opts.autoSaveMs = parseStrictNonNegativeNumber(env.YASD_AUTO_SAVE_MS, 'YASD_AUTO_SAVE_MS');
  }
  if (env.YASD_SLOW_COMMAND_MS !== undefined) {
    opts.slowCommandMs = parseStrictNonNegativeNumber(env.YASD_SLOW_COMMAND_MS, 'YASD_SLOW_COMMAND_MS');
  }
  if (env.YASD_MAX_PENDING_OUTPUT_BYTES !== undefined) {
    opts.maxPendingOutputBytes = parseStrictInteger(
      env.YASD_MAX_PENDING_OUTPUT_BYTES,
      'YASD_MAX_PENDING_OUTPUT_BYTES',
      1
    );
  }
  if (env.YASD_LOAD_ON_START !== undefined) opts.loadOnStart = env.YASD_LOAD_ON_START !== '0';
  if (env.YASD_SAVE_ON_SHUTDOWN !== undefined) opts.saveOnShutdown = env.YASD_SAVE_ON_SHUTDOWN !== '0';
  const password = env.YASD_PASSWORD ?? env.YASD_REQUIREPASS;
  if (password !== undefined && password.length > 0) opts.password = password;
  const tlsKeyPath = env.YASD_TLS_KEY;
  const tlsCertPath = env.YASD_TLS_CERT;
  const tlsCaPath = env.YASD_TLS_CA;
  const hasTlsOptions =
    Boolean(tlsKeyPath || tlsCertPath || tlsCaPath || env.YASD_TLS_MIN_VERSION) ||
    env.YASD_TLS_REQUEST_CERT !== undefined ||
    env.YASD_TLS_REJECT_UNAUTHORIZED !== undefined;
  if (hasTlsOptions) {
    if (!tlsKeyPath || !tlsCertPath) {
      throw new Error('YASD_TLS_KEY and YASD_TLS_CERT must both be set');
    }
    const tlsOptions: YasdServerTlsOptions = {
      key: fs.readFileSync(tlsKeyPath, 'utf8'),
      cert: fs.readFileSync(tlsCertPath, 'utf8'),
    };
    if (tlsCaPath) tlsOptions.ca = fs.readFileSync(tlsCaPath, 'utf8');
    if (env.YASD_TLS_MIN_VERSION) tlsOptions.minVersion = parseTlsVersion(env.YASD_TLS_MIN_VERSION);
    if (env.YASD_TLS_REQUEST_CERT !== undefined) {
      tlsOptions.requestCert = parseBoolean(env.YASD_TLS_REQUEST_CERT, 'YASD_TLS_REQUEST_CERT');
    }
    if (env.YASD_TLS_REJECT_UNAUTHORIZED !== undefined) {
      tlsOptions.rejectUnauthorized = parseBoolean(
        env.YASD_TLS_REJECT_UNAUTHORIZED,
        'YASD_TLS_REJECT_UNAUTHORIZED'
      );
    }
    opts.tls = tlsOptions;
  }
  return opts;
}

/**
 * Cache operations safe for embedded server callers.
 *
 * Mutations go through the server so AOF and invalidation hooks cannot be
 * bypassed. Persistence and transaction primitives stay server-internal.
 */
export interface YasdServerCache {
  readonly size: number;
  readonly bytesUsed: number;
  get(key: string): SnapshotEntry['value'] | undefined;
  set(key: string, value: SnapshotEntry['value'], ttlMs?: number): SnapshotEntry['value'];
  del(key: string): boolean;
  clearPrefix(prefix: string): number;
  clear(): void;
  mget(keys: string[]): Array<SnapshotEntry['value'] | undefined>;
  mset(entries: KVBatchEntry[]): number;
  incr(key: string, by?: number): number;
  decr(key: string, by?: number): number;
  cas(
    key: string,
    expected: SnapshotEntry['value'] | undefined,
    value: SnapshotEntry['value'],
    ttlMs?: number
  ): boolean;
  ttl(key: string): number;
  expiration(key: string): number | undefined | null;
  expire(key: string, ttlMs: number): boolean;
  persist(key: string): boolean;
  dump(): SnapshotEntry[];
  sweep(): number;
  stats(): KVStats;
  resetStats(): void;
  getVersion(key: string): number;
}

interface YasdServerCacheHooks {
  set(key: string, value: SnapshotEntry['value'], ttlMs?: number): SnapshotEntry['value'];
  del(key: string): boolean;
  clearPrefix(prefix: string): number;
  clear(): void;
  mset(entries: KVBatchEntry[]): number;
  incr(key: string, by?: number): number;
  decr(key: string, by?: number): number;
  cas(
    key: string,
    expected: SnapshotEntry['value'] | undefined,
    value: SnapshotEntry['value'],
    ttlMs?: number
  ): boolean;
  expire(key: string, ttlMs: number): boolean;
  persist(key: string): boolean;
}

/** Build a facade whose closure keeps the raw KVCache out of the public API. */
function createYasdServerCacheFacade(cache: KVCache, hooks: YasdServerCacheHooks): YasdServerCache {
  const facade: YasdServerCache = {
    get: key => cache.get(key),
    set: (key, value, ttlMs) => hooks.set(key, value, ttlMs),
    del: key => hooks.del(key),
    clearPrefix: prefix => hooks.clearPrefix(prefix),
    clear: () => hooks.clear(),
    mget: keys => cache.mget(keys),
    mset: entries => hooks.mset(entries),
    incr: (key, by) => hooks.incr(key, by),
    decr: (key, by) => hooks.decr(key, by),
    cas: (key, expected, value, ttlMs) => hooks.cas(key, expected, value, ttlMs),
    ttl: key => cache.ttl(key),
    expiration: key => cache.expiration(key),
    expire: (key, ttlMs) => hooks.expire(key, ttlMs),
    persist: key => hooks.persist(key),
    dump: () => cache.dump(),
    sweep: () => cache.sweep(),
    stats: () => cache.stats(),
    resetStats: () => cache.resetStats(),
    getVersion: key => cache.getVersion(key),
    get size() {
      return cache.size;
    },
    get bytesUsed() {
      return cache.bytesUsed;
    },
  };
  return Object.freeze(facade);
}

export class YasdServer {
  private kv: KVCache;
  private readonly cacheFacade: YasdServerCache;
  private hub = new PubSubHub();
  private aof: AofLog;
  private netServer?: net.Server;
  private sockets = new Set<net.Socket>();
  private autoSaveTimer?: ReturnType<typeof setInterval>;
  private persistenceQueue: Promise<void> = Promise.resolve();
  private closing = false;
  private startedAt = Date.now();
  private aofDegraded = false;
  private aofLastError?: string;

  readonly host: string;
  readonly port: number;
  readonly snapshotPath?: string;
  readonly aofPath?: string;
  readonly loadOnStart: boolean;
  readonly saveOnShutdown: boolean;
  readonly autoSaveMs: number;
  readonly maxPendingOutputBytes: number;
  readonly authRequired: boolean;
  readonly tlsEnabled: boolean;
  private password: string | undefined;
  private tlsOptions: YasdServerTlsOptions | undefined;
  private slow = new SlowLog();

  constructor(options: YasdServerOptions = {}) {
    this.host = options.host === undefined ? DEFAULT_HOST : validateHost(options.host, 'host');
    this.port = options.port === undefined ? DEFAULT_PORT : validatePort(options.port, 'port');
    this.kv = new KVCache(options.cache, key => this.onCacheExpiry(key))
    this.aof = new AofLog(options.aofPath);
    this.snapshotPath = options.snapshotPath;
    this.aofPath = options.aofPath;
    this.loadOnStart = options.loadOnStart ?? true;
    this.saveOnShutdown = options.saveOnShutdown ?? options.snapshotPath !== undefined;
    this.autoSaveMs = options.autoSaveMs === undefined
      ? 0
      : validateNonNegativeNumber(options.autoSaveMs, 'autoSaveMs');
    this.maxPendingOutputBytes = options.maxPendingOutputBytes === undefined
      ? DEFAULT_MAX_PENDING_OUTPUT_BYTES
      : validatePositiveSafeInteger(options.maxPendingOutputBytes, 'maxPendingOutputBytes');
    this.password = options.password && options.password.length > 0 ? options.password : undefined;
    this.authRequired = this.password !== undefined;
    if (options.tls && (!options.tls.key || !options.tls.cert)) {
      throw new Error('TLS requires both key and cert');
    }
    this.tlsOptions = options.tls;
    this.tlsEnabled = options.tls !== undefined;
    if (options.slowCommandMs !== undefined) {
      this.slow.setThreshold(checkSlowThreshold(options.slowCommandMs, 'slowCommandMs'));
    }
    this.cacheFacade = createYasdServerCacheFacade(this.kv, {
      set: (key, value, ttlMs) => this.cacheSet(key, value, ttlMs),
      del: key => this.cacheDel(key),
      clearPrefix: prefix => this.cacheClearPrefix(prefix),
      clear: () => this.cacheClear(),
      mset: entries => this.cacheMset(entries),
      incr: (key, by) => this.cacheIncr(key, by),
      decr: (key, by) => this.cacheDecr(key, by),
      cas: (key, expected, value, ttlMs) => this.cacheCas(key, expected, value, ttlMs),
      expire: (key, ttlMs) => this.cacheExpire(key, ttlMs),
      persist: key => this.cachePersist(key),
    });
  }

  /** Hooked cache access for embedded use. The raw cache remains private. */
  get cache(): YasdServerCache {
    return this.cacheFacade;
  }

  get pubsub(): PubSubHub {
    return this.hub;
  }

  info(): ServerInfo {
    const s = this.kv.stats();
    return {
      status: 'ok',
      version: '1.0.0',
      uptimeMs: Date.now() - this.startedAt,
      connections: this.sockets.size,
    tls: this.tlsEnabled,
    auth: this.authRequired,
      entries: s.entries,
      bytes: s.bytes,
      hits: s.hits,
      misses: s.misses,
      evictions: s.evictions,
      expiries: s.expiries,
      aofEnabled: this.aof.enabled,
      aofDegraded: this.aofDegraded,
      ...(this.aofLastError === undefined ? {} : { aofLastError: this.aofLastError }),
      aofRecoveryState: this.aof.recoveryState,
      ...(this.aof.recoveryError === undefined ? {} : { aofRecoveryError: this.aof.recoveryError }),
      subscribers: this.hub.subscriberCount(),
      channels: this.hub.channelNames(),
      slowCommandMs: this.slow.threshold,
      slowLog: this.slow.list(),
    };
  }

  /** Log commands slower than this (ms); 0 disables. */
  setSlowCommandThreshold(ms: number): void {
    this.slow.setThreshold(checkSlowThreshold(ms, 'slowCommandMs'));
  }

  /** Newest-first slow-command ring (capped at 100). */
  slowLog(): SlowEntry[] {
    return this.slow.list();
  }

  clearSlowLog(): void {
    this.slow.clear();
  }

  address(): { host: string; port: number } {
    const addr = this.netServer?.address();
    if (addr && typeof addr === 'object') {
      return { host: addr.address, port: addr.port };
    }
    return { host: this.host, port: this.port };
  }

  async start(): Promise<void> {
    if (this.netServer) return;
    if (this.loadOnStart) {
      const snapshotMetadata: SnapshotLoadMetadata = {};
      if (this.snapshotPath) {
        await loadSnapshot(this.kv, this.snapshotPath, {
          clearFirst: true,
          missingOk: true,
          metadata: snapshotMetadata,
        });
      }
      await this.aof.replay(this.kv, snapshotMetadata.aofSeq);
    }
    if (this.aof.recoveryState !== 'clean') {
      console.error(`yasd: WARNING: ${this.aof.recoveryError ?? 'AOF recovery required'}`)
    }
    this.netServer = this.tlsOptions
      ? tls.createServer(this.tlsOptions, socket => this.onConnection(socket))
      : net.createServer(socket => this.onConnection(socket));
    if (!isLoopbackHost(this.host) && !this.authRequired && !this.tlsEnabled) {
      console.warn(
        `yasd: WARNING: listening on ${this.host}:${this.port} without authentication or TLS. ` +
          'This exposes the cache to the network; use 127.0.0.1 or configure a password/TLS.'
      )
    }
    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error): void => {
        this.netServer?.off('listening', onListening);
        reject(err);
      };
      const onListening = (): void => {
        this.netServer?.off('error', onError);
        resolve();
      };
      this.netServer?.once('error', onError);
      this.netServer?.once('listening', onListening);
      this.netServer?.listen(this.port, this.host);
    });
    if (this.autoSaveMs > 0) {
      this.autoSaveTimer = setInterval(() => {
        this.save().catch(() => undefined);
      }, this.autoSaveMs);
      const t = this.autoSaveTimer as unknown as { unref?: () => void };
      if (typeof t.unref === 'function') t.unref();
    }
  }

  /** Snapshot now (and truncate the AOF, which the snapshot supersedes). */
  async save(snapshotPath?: string): Promise<number> {
    const target = snapshotPath ?? this.snapshotPath;
    if (!target) throw new Error('SAVE requires a snapshot path');
    return this.enqueuePersistence(() => this.saveUnlocked(target));
  }

  private async saveUnlocked(target: string): Promise<number> {
    const snapshotSeq = this.aof.sequence;
    const n = await saveSnapshot(this.kv, target, { aofSeq: snapshotSeq });
    try {
      this.aof.rotateAfter(snapshotSeq);
      this.aofDegraded = false;
    } catch (err) {
      throw this.aofWriteError(err);
    }
    return n;
  }

  async load(snapshotPath?: string): Promise<number> {
    const target = snapshotPath ?? this.snapshotPath;
    if (!target) throw new Error('LOAD requires a snapshot path');
    return this.enqueuePersistence(async () => {
      const count = await loadSnapshot(this.kv, target, { clearFirst: true, missingOk: false })
      this.publishInvalidate({ event: 'load' })
      return count
    })
  }

  /** Graceful shutdown: stop accepting, drain sockets, final SAVE, stop timers. */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    if (this.autoSaveTimer) {
      clearInterval(this.autoSaveTimer);
      this.autoSaveTimer = undefined;
    }
    if (this.netServer) {
      await new Promise<void>(resolve => this.netServer?.close(() => resolve()));
      this.netServer = undefined;
    }
    for (const socket of Array.from(this.sockets)) {
      try {
        socket.end();
      } catch {
        // ignore
      }
    }
    const deadline = Date.now() + 2000;
    while (this.sockets.size > 0 && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    for (const socket of Array.from(this.sockets)) {
      try {
        socket.destroy();
      } catch {
        // ignore
      }
    }
    if (this.saveOnShutdown && this.snapshotPath) {
      try {
        await this.save();
      } catch {
        // best effort on shutdown
      }
    } else {
      await this.persistenceQueue;
    }
    this.hub.unsubscribeAll();
    this.kv.close();
  }

  // ---- internals ----

  private enqueuePersistence<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.persistenceQueue.then(operation, operation);
    this.persistenceQueue = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  private onConnection(socket: net.Socket): void {
    this.sockets.add(socket);
    const state: ConnState = {
      socket,
      decoder: new RespDecoder(),
      httpBuf: null,
      subs: new Map(),
      subMode: false,
      authed: !this.authRequired,
      watchVersions: null,
      txQueue: null,
      outputQueue: [],
      outputQueueBytes: 0,
      outputBackpressured: false,
      closeWhenDrained: false,
      outputClosed: false,
    };
    socket.on('data', chunk => {
      try {
        this.onData(state, Buffer.from(chunk));
      } catch {
        this.writeReply(state, { kind: 'error', message: 'ERR protocol error' });
        this.endSocket(state);
      }
    });
    socket.on('drain', () => this.flushOutput(state));
    const cleanup = (): void => {
      this.sockets.delete(socket);
      state.outputClosed = true;
      state.outputQueue.length = 0;
      state.outputQueueBytes = 0;
      for (const [channel, listener] of state.subs) {
        this.hub.unsubscribe(channel, listener);
      }
      state.subs.clear();
    };
    socket.on('close', cleanup);
    socket.on('error', () => undefined);
  }

  /** Write immediately until Node signals backpressure, then queue bounded output. */
  private writeSocket(state: ConnState, data: Buffer): boolean {
    if (state.outputClosed || state.closeWhenDrained || state.socket.destroyed) return false;
    if (state.outputBackpressured || state.outputQueue.length > 0) {
      if (state.outputQueueBytes + data.length > this.maxPendingOutputBytes) {
        this.disconnectSlowConsumer(state);
        return false;
      }
      state.outputQueue.push(data);
      state.outputQueueBytes += data.length;
      return true;
    }
    try {
      state.outputBackpressured = !state.socket.write(data);
      return true;
    } catch {
      this.disconnectSocket(state);
      return false;
    }
  }

  private writeReply(state: ConnState, reply: RespReply | null): boolean {
    if (state.outputClosed || state.closeWhenDrained || state.socket.destroyed) return false;
    return this.writeSocket(state, encodeReply(reply));
  }

  private flushOutput(state: ConnState): void {
    if (state.outputClosed) return;
    state.outputBackpressured = false;
    while (state.outputQueue.length > 0) {
      const data = state.outputQueue.shift() as Buffer;
      state.outputQueueBytes -= data.length;
      try {
        if (!state.socket.write(data)) {
          state.outputBackpressured = true;
          return;
        }
      } catch {
        this.disconnectSocket(state);
        return;
      }
    }
    if (state.closeWhenDrained) {
      state.outputClosed = true;
      state.socket.end();
    }
  }

  private endSocket(state: ConnState): void {
    if (state.outputClosed) return;
    state.closeWhenDrained = true;
    if (!state.outputBackpressured && state.outputQueue.length === 0) {
      state.outputClosed = true;
      state.socket.end();
    }
  }

  private disconnectSlowConsumer(state: ConnState): void {
    this.disconnectSocket(state);
  }

  private disconnectSocket(state: ConnState): void {
    if (state.outputClosed) return;
    state.outputClosed = true;
    state.closeWhenDrained = false;
    state.outputQueue.length = 0;
    state.outputQueueBytes = 0;
    state.socket.destroy();
  }

  private onData(state: ConnState, chunk: Buffer): void {
    // Single-port HTTP: a connection starting with `GET ` is a health check.
    if (state.httpBuf !== null) {
      this.onHttpData(state, chunk);
      return;
    }
    if (chunk.length >= 4 && chunk.toString('utf8', 0, 4) === 'GET ') {
      state.httpBuf = chunk;
      this.onHttpData(state, Buffer.alloc(0));
      return;
    }
    const requests = state.decoder.push(chunk);
    for (const request of requests) {
      if (state.outputClosed) return;
      const done = this.onRequest(state, request);
      if (done === 'close') return;
    }
  }

  private onHttpData(state: ConnState, chunk: Buffer): void {
    state.httpBuf = Buffer.concat([state.httpBuf ?? Buffer.alloc(0), chunk]);
    const end = state.httpBuf.indexOf('\r\n\r\n');
    if (end === -1) {
      if (state.httpBuf.length > 16 * 1024) state.socket.destroy();
      return; // wait for full headers
    }
    const head = state.httpBuf.toString('utf8', 0, end);
    const requestLine = head.split('\r\n')[0] ?? '';
    const path = requestLine.split(' ')[1] ?? '/';
    if (path === '/healthz' || path === '/health') {
      const body = Buffer.from(JSON.stringify(this.info()), 'utf8');
      this.writeSocket(state,
        Buffer.concat([
          Buffer.from(
            'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n' +
              `Content-Length: ${body.length}\r\nConnection: close\r\n\r\n`,
            'utf8'
          ),
          body,
        ]));
    } else {
      const body = Buffer.from('{"error":"not found"}', 'utf8');
      this.writeSocket(state,
        Buffer.concat([
          Buffer.from(
            'HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n' +
              `Content-Length: ${body.length}\r\nConnection: close\r\n\r\n`,
            'utf8'
          ),
          body,
        ]));
    }
    this.endSocket(state);
  }

  /** Returns 'close' when the connection was ended (QUIT). */
  private onRequest(state: ConnState, request: RespReply): 'ok' | 'close' {
    let argv: string[];
    try {
      argv = requestArgv(request);
    } catch (err) {
      this.writeReply(state, { kind: 'error', message: `ERR ${(err as Error).message}` });
      return 'ok';
    }
    const cmd = (argv[0] ?? '').toUpperCase();
    if (!state.authed && cmd !== 'AUTH' && cmd !== 'QUIT') {
      this.writeReply(state, { kind: 'error', message: 'NOAUTH Authentication required (send AUTH first)' });
      return 'ok';
    }
    if (state.subMode && cmd !== 'SUBSCRIBE' && cmd !== 'UNSUBSCRIBE' && cmd !== 'PING' && cmd !== 'QUIT' && cmd !== 'AUTH') {
      this.writeReply(state, { kind: 'error', message: 'ERR only AUTH/SUBSCRIBE/UNSUBSCRIBE/PING/QUIT allowed in subscriber mode' });
      return 'ok';
    }
    const started = performance.now();
    try {
      const reply = this.dispatch(state, cmd, argv.slice(1));
      if (reply === 'close') {
        this.writeReply(state, { kind: 'simple', value: 'OK' });
        this.endSocket(state);
        return 'close';
      }
      if (reply !== 'silent') {
        this.writeReply(state, reply);
      }
    } catch (err) {
      this.writeReply(state, { kind: 'error', message: `ERR ${(err as Error).message}` });
    } finally {
      // SAVE/LOAD finish asynchronously; the sync portion is what's timed.
      this.slow.record(cmd, performance.now() - started, argv.length - 1);
    }
    return 'ok';
  }

  private appendAof(op: AofOp): void {
    if (!this.aof.enabled) return;
    try {
      this.aof.append(op);
      this.aofDegraded = false;
    } catch (err) {
      throw this.aofWriteError(err);
    }
  }

  private aofWriteError(err: unknown): Error {
    const detail = err instanceof Error ? err.message : String(err);
    this.aofDegraded = true;
    this.aofLastError = detail;
    return new Error(`AOF write failed: ${detail}`);
  }

  private logAof(op: AofMutation, effects?: TransactionEffects): void {
    if (effects) {
      effects.aof.push(op);
      return;
    }
    this.appendAof(op);
  }

  private publishInvalidate(event: InvalidationEvent, effects?: TransactionEffects): void {
    if (effects) {
      effects.invalidations.push(event);
      return;
    }
    try {
      this.hub.publish(INVALIDATE_CHANNEL, invalidateMessage(event));
    } catch {
      // ignore
    }
  }

  private onCacheExpiry(key: string): void {
    this.publishInvalidate({ event: 'expire', key })
  }

  private runCacheMutation<T>(operation: (effects: TransactionEffects) => T): T {
    const effects: TransactionEffects = { aof: [], invalidations: [] };
    const commit = (): T => {
      const result = operation(effects);
      if (effects.aof.length === 1) {
        this.appendAof(effects.aof[0] as AofOp);
      } else if (effects.aof.length > 1) {
        this.appendAof({ op: 'transaction', ops: effects.aof });
      }
      return result;
    };
    const result = this.aof.enabled ? this.kv.atomic(commit) : commit();
    for (const event of effects.invalidations) {
      this.publishInvalidate(event);
    }
    return result;
  }

  private cacheSet(
    key: string,
    value: SnapshotEntry['value'],
    ttlMs?: number
  ): SnapshotEntry['value'] {
    return this.runCacheMutation(effects => {
      const result = this.kv.set(key, value, ttlMs);
      this.logAof({ op: 'set', key, value, expiresAt: this.aofExpiry(key) }, effects);
      this.publishInvalidate({ event: 'set', key }, effects);
      return result;
    });
  }

  private cacheDel(key: string): boolean {
    return this.runCacheMutation(effects => {
      const deleted = this.kv.del(key);
      if (deleted) {
        this.logAof({ op: 'del', keys: [key] }, effects);
        this.publishInvalidate({ event: 'del', key }, effects);
      }
      return deleted;
    });
  }

  private cacheClearPrefix(prefix: string): number {
    return this.runCacheMutation(effects => {
      const count = this.kv.clearPrefix(prefix);
      this.logAof({ op: 'clear', prefix }, effects);
      this.publishInvalidate({ event: 'clear', prefix }, effects);
      return count;
    });
  }

  private cacheClear(): void {
    this.runCacheMutation(effects => {
      const keys = this.kv.dump().map(entry => entry.key);
      this.kv.clear();
      if (keys.length > 0) {
        this.logAof({ op: 'del', keys }, effects);
        for (const key of keys) {
          this.publishInvalidate({ event: 'del', key }, effects);
        }
      }
    });
  }

  private cacheMset(entries: KVBatchEntry[]): number {
    return this.runCacheMutation(effects => {
      const count = this.kv.mset(entries);
      const aofEntries: AofBatchEntry[] = entries.map(entry => ({
        ...entry,
        expiresAt: this.aofExpiry(entry.key),
      }));
      this.logAof({ op: 'mset', entries: aofEntries }, effects);
      for (const entry of entries) {
        this.publishInvalidate({ event: 'set', key: entry.key }, effects);
      }
      return count;
    });
  }

  private cacheIncr(key: string, by = 1): number {
    return this.runCacheMutation(effects => {
      const next = this.kv.incr(key, by);
      this.logAof({ op: 'incr', key, by }, effects);
      this.publishInvalidate({ event: 'set', key }, effects);
      return next;
    });
  }

  private cacheDecr(key: string, by = 1): number {
    return this.runCacheMutation(effects => {
      const next = this.kv.decr(key, by);
      this.logAof({ op: 'incr', key, by: -by }, effects);
      this.publishInvalidate({ event: 'set', key }, effects);
      return next;
    });
  }

  private cacheCas(
    key: string,
    expected: SnapshotEntry['value'] | undefined,
    value: SnapshotEntry['value'],
    ttlMs?: number
  ): boolean {
    return this.runCacheMutation(effects => {
      const ok = this.kv.cas(key, expected, value, ttlMs);
      if (ok) {
        const expiresAt = this.kv.expiration(key);
        if (expiresAt === null) {
          this.logAof({ op: 'del', keys: [key] }, effects);
        } else {
          this.logAof({ op: 'set', key, value, expiresAt: expiresAt ?? null }, effects);
        }
        this.publishInvalidate({ event: 'set', key }, effects);
      }
      return ok;
    });
  }

  private cacheExpire(key: string, ttlMs: number): boolean {
    return this.runCacheMutation(effects => {
      const ok = this.kv.expire(key, ttlMs);
      if (ok) {
        this.logAof({ op: 'expire', key, expiresAt: this.aofExpiry(key) ?? 0 }, effects);
        this.publishInvalidate({ event: 'expire', key }, effects);
      }
      return ok;
    });
  }

  private cachePersist(key: string): boolean {
    return this.runCacheMutation(effects => {
      const ok = this.kv.persist(key);
      if (ok) {
        this.logAof({ op: 'persist', key }, effects);
        this.publishInvalidate({ event: 'persist', key }, effects);
      }
      return ok;
    });
  }

  private parseValue(json: string): SnapshotEntry['value'] {
    return JSON.parse(json) as SnapshotEntry['value'];
  }

  /** AOF TTL marker: null persists, 0 means the key was already gone. */
  private aofExpiry(key: string): number | null {
    const expiresAt = this.kv.expiration(key);
    return expiresAt === undefined ? null : expiresAt === null ? 0 : expiresAt;
  }

  /**
   * Transaction entry point: WATCH/UNWATCH/MULTI/EXEC/DISCARD plus queueing
   * while in MULTI. Everything else delegates to `executeCommand`.
   */
  private dispatch(state: ConnState, cmd: string, args: string[]): RespReply | 'silent' | 'close' {
    switch (cmd) {
      case 'WATCH': {
        if (state.subMode) throw new Error('WATCH not allowed in subscriber mode');
        if (state.txQueue !== null) throw new Error('WATCH inside MULTI is not allowed');
        this.requireArgs(cmd, args, 1, Infinity);
        if (state.watchVersions === null) state.watchVersions = new Map();
        for (const key of args) state.watchVersions.set(key, this.kv.getVersion(key));
        return { kind: 'simple', value: 'OK' };
      }
      case 'UNWATCH': {
        if (args.length > 0) throw new Error('UNWATCH takes no arguments');
        state.watchVersions = null;
        return { kind: 'simple', value: 'OK' };
      }
      case 'MULTI': {
        if (args.length > 0) throw new Error('MULTI takes no arguments');
        if (state.subMode) throw new Error('MULTI not allowed in subscriber mode');
        if (state.txQueue !== null) throw new Error('MULTI calls cannot nest');
        state.txQueue = [];
        return { kind: 'simple', value: 'OK' };
      }
      case 'DISCARD': {
        if (args.length > 0) throw new Error('DISCARD takes no arguments');
        if (state.txQueue === null) throw new Error('DISCARD without MULTI');
        state.txQueue = null;
        state.watchVersions = null;
        return { kind: 'simple', value: 'OK' };
      }
      case 'EXEC': {
        if (args.length > 0) throw new Error('EXEC takes no arguments');
        if (state.txQueue === null) throw new Error('EXEC without MULTI');
        return this.execTransaction(state);
      }
      default: {
        // Inside MULTI only the KV ops (plus PING) queue; everything else
        // (AUTH, SUBSCRIBE, SAVE/LOAD, ...) is rejected so a commit stays
        // a synchronous, atomic KV batch.
        if (state.txQueue !== null) {
          if (!TX_QUEUEABLE.has(cmd)) {
            throw new Error(`${cmd} not allowed inside MULTI`);
          }
          state.txQueue.push({ cmd, args });
          return { kind: 'simple', value: 'QUEUED' };
        }
        if (this.aof.enabled && AOF_COMMANDS.has(cmd)) {
          return this.kv.atomic(() => this.executeCommand(state, cmd, args));
        }
        return this.executeCommand(state, cmd, args);
      }
    }
  }

  /**
   * Commit the queued transaction. Returns an array of per-op replies, or
   * nil (`*-1`) when a watched key changed — in which case nothing is
   * applied. A command error rolls back the complete batch. AOF and
   * invalidations flush only after the cache commit succeeds.
   */
  private execTransaction(state: ConnState): RespReply {
    const queue = state.txQueue ?? [];
    const watched = state.watchVersions;
    state.txQueue = null;
    state.watchVersions = null;
    if (watched !== null) {
      for (const [key, version] of watched) {
        if (this.kv.getVersion(key) !== version) {
          return { kind: 'nil' };
        }
      }
    }
    const items: Array<RespReply | null> = [];
    const effects: TransactionEffects = { aof: [], invalidations: [] };
    const abort = Symbol('transaction aborted');
    try {
      this.kv.atomic(() => {
        let failed = false;
        for (const op of queue) {
          try {
            const reply = this.executeCommand(state, op.cmd, op.args, effects);
            if (reply === 'silent' || reply === 'close') {
              failed = true;
              items.push({ kind: 'error', message: `${op.cmd} cannot run inside MULTI` });
            } else {
              items.push(reply);
            }
          } catch (err) {
            failed = true;
            items.push({ kind: 'error', message: `ERR ${(err as Error).message}` });
          }
        }
        if (failed) throw abort;
        if (effects.aof.length > 0) {
          this.appendAof({ op: 'transaction', ops: effects.aof });
        }
      });
    } catch (err) {
      if (err !== abort) throw err;
      return { kind: 'array', items };
    }
    for (const event of effects.invalidations) {
      this.publishInvalidate(event);
    }
    return { kind: 'array', items };
  }

  private executeCommand(
    state: ConnState,
    cmd: string,
    args: string[],
    effects?: TransactionEffects
  ): RespReply | 'silent' | 'close' {
    switch (cmd) {
      case 'PING':
        return args.length > 0
          ? { kind: 'bulk', value: args.join(' ') }
          : { kind: 'simple', value: 'PONG' };

      case 'QUIT':
        return 'close';

      case 'AUTH': {
        if (args.length !== 1) throw new Error('AUTH takes one password');
        if (!this.authRequired) return { kind: 'simple', value: 'OK' };
        if (args[0] === this.password) {
          state.authed = true;
          return { kind: 'simple', value: 'OK' };
        }
        throw new Error('invalid password');
      }

      case 'CAS': {
        // CAS key <expectedJson|empty=assert-missing> <valueJson> [PX ms]
        if (args.length < 3 || args.length > 5) {
          throw new Error('CAS syntax: CAS key expectedJson valueJson [PX ms]');
        }
        const key = args[0] as string;
        const expectedRaw = args[1] as string;
        const value = this.parseValue(args[2] as string);
        const ttlMs = this.parsePx(args.slice(3));
        const expected = expectedRaw === '' ? undefined : (JSON.parse(expectedRaw) as SnapshotEntry['value']);
        const ok = this.kv.cas(key, expected, value, ttlMs);
        if (ok) {
          const expiresAt = this.kv.expiration(key);
          if (expiresAt === null) {
            this.logAof({ op: 'del', keys: [key] }, effects);
          } else {
            this.logAof({ op: 'set', key, value, expiresAt: expiresAt ?? null }, effects);
          }
          this.publishInvalidate({ event: 'set', key }, effects);
        }
        return { kind: 'int', value: ok ? 1 : 0 };
      }

      case 'GET': {
        this.requireArgs(cmd, args, 1);
        const value = this.kv.get(args[0] as string);
        if (value === undefined) return { kind: 'bulk', value: null };
        return { kind: 'bulk', value: JSON.stringify(value) ?? 'null' };
      }

      case 'SET': {
        this.requireArgs(cmd, args, 2, 4);
        const key = args[0] as string;
        const value = this.parseValue(args[1] as string);
        const ttlMs = this.parsePx(args.slice(2));
        this.kv.set(key, value, ttlMs);
        this.logAof({ op: 'set', key, value, expiresAt: this.aofExpiry(key) }, effects);
        this.publishInvalidate({ event: 'set', key }, effects);
        return { kind: 'simple', value: 'OK' };
      }

      case 'MSET': {
        if (args.length === 0 || args.length % 2 !== 0) {
          throw new Error('MSET requires key/value pairs');
        }
        const entries: KVBatchEntry[] = [];
        for (let i = 0; i < args.length; i += 2) {
          entries.push({ key: args[i] as string, value: this.parseValue(args[i + 1] as string) });
        }
        this.kv.mset(entries);
        const aofEntries: AofBatchEntry[] = entries.map(entry => ({
          ...entry,
          expiresAt: this.aofExpiry(entry.key),
        }));
        this.logAof({ op: 'mset', entries: aofEntries }, effects);
        for (const e of entries) this.publishInvalidate({ event: 'set', key: e.key }, effects);
        return { kind: 'simple', value: 'OK' };
      }

      case 'MGET': {
        const items = this.kv.mget(args).map(v =>
          v === undefined
            ? { kind: 'bulk', value: null }
            : { kind: 'bulk', value: JSON.stringify(v) ?? 'null' }
        );
        return { kind: 'array', items } as RespReply;
      }

      case 'DEL': {
        let count = 0;
        const deleted: string[] = [];
        for (const key of args) {
          if (this.kv.del(key)) {
            count++;
            deleted.push(key);
          }
        }
        if (deleted.length > 0) this.logAof({ op: 'del', keys: deleted }, effects);
        for (const key of deleted) this.publishInvalidate({ event: 'del', key }, effects);
        return { kind: 'int', value: count };
      }

      case 'CLEAR': {
        this.requireArgs(cmd, args, 1);
        const prefix = args[0] as string;
        const count = this.kv.clearPrefix(prefix);
        this.logAof({ op: 'clear', prefix }, effects);
        this.publishInvalidate({ event: 'clear', prefix }, effects);
        return { kind: 'int', value: count };
      }

      case 'TTL': {
        this.requireArgs(cmd, args, 1);
        return { kind: 'int', value: this.kv.ttl(args[0] as string) };
      }

      case 'EXPIRE': {
        this.requireArgs(cmd, args, 2);
        const ttlMs = this.parseMs(args[1] as string);
        const ok = this.kv.expire(args[0] as string, ttlMs);
        if (ok) {
          this.logAof({ op: 'expire', key: args[0] as string, expiresAt: this.aofExpiry(args[0] as string) ?? 0 }, effects);
          this.publishInvalidate({ event: 'expire', key: args[0] as string }, effects);
        }
        return { kind: 'int', value: ok ? 1 : 0 };
      }

      case 'PERSIST': {
        this.requireArgs(cmd, args, 1);
        const ok = this.kv.persist(args[0] as string);
        if (ok) {
          this.logAof({ op: 'persist', key: args[0] as string }, effects)
          this.publishInvalidate({ event: 'persist', key: args[0] as string }, effects)
        }
        return { kind: 'int', value: ok ? 1 : 0 };
      }

      case 'INCR':
      case 'DECR': {
        if (args.length < 1 || args.length > 2) throw new Error(`${cmd} requires a key and optional delta`);
        const rawBy = args[1] === undefined ? 1 : parseStrictNumber(args[1], 'delta');
        const next = cmd === 'INCR' ? this.kv.incr(args[0] as string, rawBy) : this.kv.decr(args[0] as string, rawBy);
        this.logAof({ op: 'incr', key: args[0] as string, by: cmd === 'INCR' ? rawBy : -rawBy }, effects);
        this.publishInvalidate({ event: 'set', key: args[0] as string }, effects);
        return Number.isInteger(next)
          ? { kind: 'int', value: next }
          : { kind: 'bulk', value: JSON.stringify(next) };
      }

      case 'PUBLISH': {
        this.requireArgs(cmd, args, 2);
        const count = this.hub.publish(args[0] as string, args[1] as string);
        return { kind: 'int', value: count };
      }

      case 'SUBSCRIBE': {
        this.requireArgs(cmd, args, 1, Infinity);
        state.subMode = true;
        const acks: Array<RespReply | null> = [];
        for (const channel of args) {
          if (!state.subs.has(channel)) {
            const listener: PubSubListener = (ch, message) => {
              this.writeReply(state, {
                kind: 'array',
                items: [
                  { kind: 'bulk', value: 'message' },
                  { kind: 'bulk', value: ch },
                  { kind: 'bulk', value: message },
                ],
              });
            };
            state.subs.set(channel, listener);
            this.hub.subscribe(channel, listener);
          }
          acks.push({
            kind: 'array',
            items: [
              { kind: 'bulk', value: 'subscribe' },
              { kind: 'bulk', value: channel },
              { kind: 'int', value: state.subs.size },
            ],
          });
        }
        // Multiple acks, one per channel (Redis-compatible).
        for (const ack of acks) {
          if (!this.writeReply(state, ack)) break;
        }
        return 'silent';
      }

      case 'UNSUBSCRIBE': {
        const channels = args.length === 0 ? Array.from(state.subs.keys()) : args;
        for (const channel of channels) {
          const listener = state.subs.get(channel);
          if (listener) {
            this.hub.unsubscribe(channel, listener);
            state.subs.delete(channel);
          }
          if (!this.writeReply(state, {
            kind: 'array',
            items: [
              { kind: 'bulk', value: 'unsubscribe' },
              { kind: 'bulk', value: channel },
              { kind: 'int', value: state.subs.size },
            ],
          })) break;
        }
        if (state.subs.size === 0) state.subMode = false;
        return 'silent';
      }

      case 'INFO': {
        return { kind: 'bulk', value: JSON.stringify(this.info()) };
      }

      case 'SAVE': {
        if (args.length > 1) throw new Error('SAVE takes an optional path');
        const path = args[0];
        // save() is async; block the event loop minimally by chaining.
        // To keep command handling synchronous, run and reply when done.
        this.save(path)
          .then(
            () => this.writeReply(state, { kind: 'simple', value: 'OK' }),
            err => this.writeReply(state, { kind: 'error', message: `ERR ${(err as Error).message}` })
          )
          .catch(() => undefined);
        return 'silent';
      }

      case 'LOAD': {
        if (args.length > 1) throw new Error('LOAD takes an optional path');
        const path = args[0];
        this.load(path)
          .then(
            count => this.writeReply(state, { kind: 'simple', value: `OK ${count}` }),
            err => this.writeReply(state, { kind: 'error', message: `ERR ${(err as Error).message}` })
          )
          .catch(() => undefined);
        return 'silent';
      }

      default:
        throw new Error(`unknown command: ${cmd}`);
    }
  }

  private requireArgs(cmd: string, args: string[], min: number, max: number = min): void {
    if (args.length < min || args.length > max) {
      throw new Error(`${cmd} takes ${min === max ? min : `${min}..${max}`} argument(s), got ${args.length}`);
    }
  }

  private parsePx(rest: string[]): number | undefined {
    if (rest.length === 0) return undefined;
    if (rest.length !== 2 || rest[0]?.toUpperCase() !== 'PX') {
      throw new Error('SET syntax: SET key value [PX ms]');
    }
    return this.parseMs(rest[1] as string);
  }

  private parseMs(raw: string): number {
    return parseStrictNonNegativeNumber(raw, 'TTL');
  }
}

// Re-exported so `encodeCommand` users (tests, client) share one import root.
export { encodeCommand };
