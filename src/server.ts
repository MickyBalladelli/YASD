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
  SnapshotEntry,
} from './cache';
import { PubSubHub, PubSubListener, INVALIDATE_CHANNEL, invalidateMessage } from './pubsub';
import { saveSnapshot, loadSnapshot, AofLog, AofOp } from './persistence';
import {
  RespDecoder,
  RespReply,
  encodeReply,
  encodeCommand,
  requestArgv,
} from './protocol';

export const DEFAULT_PORT = 7379;

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
  /**
   * Password for the AUTH command. When set, every command except AUTH/QUIT
   * is rejected with NOAUTH until authenticated. HTTP /healthz stays open
   * (load balancers shouldn't need the secret).
   */
  password?: string;
  /** TLS identity (PEM contents). When set the port serves TLS. */
  tls?: { key: string | Buffer; cert: string | Buffer };
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
}

function parsePort(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  const n = parseInt(value, 10);
  if (!Number.isInteger(n) || n < 0 || n > 65535) throw new Error(`invalid port: ${value}`);
  return n;
}

export function serverOptionsFromEnv(env: NodeJS.ProcessEnv = process.env): YasdServerOptions {
  const cache: KVOptions = {};
  if (env.CACHE_MAX_ENTRIES !== undefined) cache.maxEntries = parseInt(env.CACHE_MAX_ENTRIES, 10);
  if (env.CACHE_MAX_BYTES !== undefined) cache.maxBytes = parseInt(env.CACHE_MAX_BYTES, 10);
  if (env.CACHE_DEFAULT_TTL_MS !== undefined) cache.defaultTTLMs = parseInt(env.CACHE_DEFAULT_TTL_MS, 10);
  if (env.CACHE_NAMESPACE_TTLS !== undefined) {
    try {
      cache.namespaceTTLMs = JSON.parse(env.CACHE_NAMESPACE_TTLS) as Record<string, number>;
    } catch {
      throw new Error('CACHE_NAMESPACE_TTLS must be JSON, e.g. {"feeds":15000}');
    }
  }
  const opts: YasdServerOptions = {
    host: env.YASD_HOST ?? '0.0.0.0',
    port: parsePort(env.YASD_PORT, DEFAULT_PORT),
    cache,
  };
  if (env.YASD_SNAPSHOT) opts.snapshotPath = env.YASD_SNAPSHOT;
  if (env.YASD_AOF) opts.aofPath = env.YASD_AOF;
  if (env.YASD_AUTO_SAVE_MS !== undefined) opts.autoSaveMs = parseInt(env.YASD_AUTO_SAVE_MS, 10);
  if (env.YASD_SLOW_COMMAND_MS !== undefined) {
    const n = parseFloat(env.YASD_SLOW_COMMAND_MS);
    if (!(n >= 0)) throw new Error('YASD_SLOW_COMMAND_MS must be a number >= 0');
    opts.slowCommandMs = n;
  }
  if (env.YASD_LOAD_ON_START !== undefined) opts.loadOnStart = env.YASD_LOAD_ON_START !== '0';
  if (env.YASD_SAVE_ON_SHUTDOWN !== undefined) opts.saveOnShutdown = env.YASD_SAVE_ON_SHUTDOWN !== '0';
  const password = env.YASD_PASSWORD ?? env.YASD_REQUIREPASS;
  if (password !== undefined && password.length > 0) opts.password = password;
  const tlsKeyPath = env.YASD_TLS_KEY;
  const tlsCertPath = env.YASD_TLS_CERT;
  if (tlsKeyPath || tlsCertPath) {
    if (!tlsKeyPath || !tlsCertPath) {
      throw new Error('YASD_TLS_KEY and YASD_TLS_CERT must both be set');
    }
    opts.tls = {
      key: fs.readFileSync(tlsKeyPath, 'utf8'),
      cert: fs.readFileSync(tlsCertPath, 'utf8'),
    };
  }
  return opts;
}

export class YasdServer {
  private kv: KVCache;
  private hub = new PubSubHub();
  private aof: AofLog;
  private netServer?: net.Server;
  private sockets = new Set<net.Socket>();
  private autoSaveTimer?: ReturnType<typeof setInterval>;
  private closing = false;
  private startedAt = Date.now();

  readonly host: string;
  readonly port: number;
  readonly snapshotPath?: string;
  readonly aofPath?: string;
  readonly loadOnStart: boolean;
  readonly saveOnShutdown: boolean;
  readonly autoSaveMs: number;
  readonly authRequired: boolean;
  readonly tlsEnabled: boolean;
  private password: string | undefined;
  private tlsOptions: { key: string | Buffer; cert: string | Buffer } | undefined;
  private slow = new SlowLog();

  constructor(options: YasdServerOptions = {}) {
    this.host = options.host ?? '0.0.0.0';
    this.port = options.port ?? DEFAULT_PORT;
    this.kv = new KVCache(options.cache);
    this.aof = new AofLog(options.aofPath);
    this.snapshotPath = options.snapshotPath;
    this.aofPath = options.aofPath;
    this.loadOnStart = options.loadOnStart ?? true;
    this.saveOnShutdown = options.saveOnShutdown ?? options.snapshotPath !== undefined;
    this.autoSaveMs = options.autoSaveMs ?? 0;
    this.password = options.password && options.password.length > 0 ? options.password : undefined;
    this.authRequired = this.password !== undefined;
    this.tlsOptions = options.tls;
    this.tlsEnabled = options.tls !== undefined;
    if (options.slowCommandMs !== undefined) {
      this.slow.setThreshold(checkSlowThreshold(options.slowCommandMs, 'slowCommandMs'));
    }
  }

  /** Direct access to the underlying cache (embedded use, tests). */
  get cache(): KVCache {
    return this.kv;
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
      if (this.snapshotPath) {
        await loadSnapshot(this.kv, this.snapshotPath, { clearFirst: true, missingOk: true });
      }
      await this.aof.replay(this.kv);
    }
    this.netServer = this.tlsOptions
      ? tls.createServer({ key: this.tlsOptions.key, cert: this.tlsOptions.cert }, socket =>
          this.onConnection(socket)
        )
      : net.createServer(socket => this.onConnection(socket));
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
    const n = await saveSnapshot(this.kv, target);
    await this.aof.truncate();
    return n;
  }

  async load(snapshotPath?: string): Promise<number> {
    const target = snapshotPath ?? this.snapshotPath;
    if (!target) throw new Error('LOAD requires a snapshot path');
    return loadSnapshot(this.kv, target, { clearFirst: true, missingOk: false });
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
    }
    this.hub.unsubscribeAll();
    this.kv.close();
  }

  // ---- internals ----

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
    };
    socket.on('data', chunk => {
      try {
        this.onData(state, Buffer.from(chunk));
      } catch {
        try {
          socket.write(encodeReply({ kind: 'error', message: 'ERR protocol error' }));
        } catch {
          // ignore
        }
        socket.destroy();
      }
    });
    const cleanup = (): void => {
      this.sockets.delete(socket);
      for (const [channel, listener] of state.subs) {
        this.hub.unsubscribe(channel, listener);
      }
      state.subs.clear();
    };
    socket.on('close', cleanup);
    socket.on('error', () => undefined);
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
      state.socket.write(
        Buffer.concat([
          Buffer.from(
            'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n' +
              `Content-Length: ${body.length}\r\nConnection: close\r\n\r\n`,
            'utf8'
          ),
          body,
        ])
      );
    } else {
      const body = Buffer.from('{"error":"not found"}', 'utf8');
      state.socket.write(
        Buffer.concat([
          Buffer.from(
            'HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n' +
              `Content-Length: ${body.length}\r\nConnection: close\r\n\r\n`,
            'utf8'
          ),
          body,
        ])
      );
    }
    state.socket.end();
  }

  /** Returns 'close' when the connection was ended (QUIT). */
  private onRequest(state: ConnState, request: RespReply): 'ok' | 'close' {
    let argv: string[];
    try {
      argv = requestArgv(request);
    } catch (err) {
      state.socket.write(encodeReply({ kind: 'error', message: `ERR ${(err as Error).message}` }));
      return 'ok';
    }
    const cmd = (argv[0] ?? '').toUpperCase();
    if (!state.authed && cmd !== 'AUTH' && cmd !== 'QUIT') {
      state.socket.write(
        encodeReply({ kind: 'error', message: 'NOAUTH Authentication required (send AUTH first)' })
      );
      return 'ok';
    }
    if (state.subMode && cmd !== 'SUBSCRIBE' && cmd !== 'UNSUBSCRIBE' && cmd !== 'PING' && cmd !== 'QUIT' && cmd !== 'AUTH') {
      state.socket.write(
        encodeReply({ kind: 'error', message: 'ERR only AUTH/SUBSCRIBE/UNSUBSCRIBE/PING/QUIT allowed in subscriber mode' })
      );
      return 'ok';
    }
    const started = performance.now();
    try {
      const reply = this.dispatch(state, cmd, argv.slice(1));
      if (reply === 'close') {
        state.socket.write(encodeReply({ kind: 'simple', value: 'OK' }));
        state.socket.end();
        return 'close';
      }
      if (reply !== 'silent') {
        state.socket.write(encodeReply(reply));
      }
    } catch (err) {
      state.socket.write(encodeReply({ kind: 'error', message: `ERR ${(err as Error).message}` }));
    } finally {
      // SAVE/LOAD finish asynchronously; the sync portion is what's timed.
      this.slow.record(cmd, performance.now() - started, argv.length - 1);
    }
    return 'ok';
  }

  private logAof(op: AofOp): void {
    try {
      this.aof.append(op);
    } catch {
      // AOF failures must not break serving; snapshot still works.
    }
  }

  private publishInvalidate(event: { event: 'set' | 'del' | 'clear' | 'expire'; key?: string; prefix?: string }): void {
    try {
      this.hub.publish(INVALIDATE_CHANNEL, invalidateMessage(event));
    } catch {
      // ignore
    }
  }

  private parseValue(json: string): SnapshotEntry['value'] {
    return JSON.parse(json) as SnapshotEntry['value'];
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
        return this.executeCommand(state, cmd, args);
      }
    }
  }

  /**
   * Commit the queued transaction. Returns an array of per-op replies, or
   * nil (`*-1`) when a watched key changed — in which case nothing is
   * applied. Watches clear either way. Single-threaded dispatch makes the
   * apply loop uninterrupted (atomic).
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
    for (const op of queue) {
      try {
        const reply = this.executeCommand(state, op.cmd, op.args);
        if (reply === 'silent' || reply === 'close') {
          items.push({ kind: 'error', message: `${op.cmd} cannot run inside MULTI` });
        } else {
          items.push(reply);
        }
      } catch (err) {
        items.push({ kind: 'error', message: `ERR ${(err as Error).message}` });
      }
    }
    return { kind: 'array', items };
  }

  private executeCommand(state: ConnState, cmd: string, args: string[]): RespReply | 'silent' | 'close' {
    switch (cmd) {
      case 'PING':
        return args.length > 0
          ? { kind: 'bulk', value: args.join(' ') }
          : { kind: 'simple', value: 'PONG' };

      case 'QUIT':
        return 'close';

      case 'AUTH': {
        if (args.length > 1) throw new Error('AUTH takes an optional password');
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
          // AOF replay uses relative TTL; convert an absolute preserved
          // expiry to remaining ms so the log stays meaningful.
          const remaining = this.kv.ttl(key);
          this.logAof({
            op: 'set',
            key,
            value,
            ...(remaining >= 0 ? { ttlMs: remaining } : {}),
          });
          this.publishInvalidate({ event: 'set', key });
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
        this.logAof({ op: 'set', key, value, ...(ttlMs === undefined ? {} : { ttlMs }) });
        this.publishInvalidate({ event: 'set', key });
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
        this.logAof({ op: 'mset', entries });
        for (const e of entries) this.publishInvalidate({ event: 'set', key: e.key });
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
        if (deleted.length > 0) this.logAof({ op: 'del', keys: deleted });
        for (const key of deleted) this.publishInvalidate({ event: 'del', key });
        return { kind: 'int', value: count };
      }

      case 'CLEAR': {
        this.requireArgs(cmd, args, 1);
        const prefix = args[0] as string;
        const count = this.kv.clearPrefix(prefix);
        this.logAof({ op: 'clear', prefix });
        this.publishInvalidate({ event: 'clear', prefix });
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
          this.logAof({ op: 'expire', key: args[0] as string, ttlMs });
          this.publishInvalidate({ event: 'expire', key: args[0] as string });
        }
        return { kind: 'int', value: ok ? 1 : 0 };
      }

      case 'PERSIST': {
        this.requireArgs(cmd, args, 1);
        const ok = this.kv.persist(args[0] as string);
        if (ok) this.logAof({ op: 'persist', key: args[0] as string });
        return { kind: 'int', value: ok ? 1 : 0 };
      }

      case 'INCR':
      case 'DECR': {
        if (args.length < 1 || args.length > 2) throw new Error(`${cmd} requires a key and optional delta`);
        const rawBy = args[1] === undefined ? 1 : Number(args[1]);
        if (!Number.isFinite(rawBy)) throw new Error('delta must be a finite number');
        const next = cmd === 'INCR' ? this.kv.incr(args[0] as string, rawBy) : this.kv.decr(args[0] as string, rawBy);
        this.logAof({ op: 'incr', key: args[0] as string, by: cmd === 'INCR' ? rawBy : -rawBy });
        this.publishInvalidate({ event: 'set', key: args[0] as string });
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
              try {
                state.socket.write(
                  encodeReply({
                    kind: 'array',
                    items: [
                      { kind: 'bulk', value: 'message' },
                      { kind: 'bulk', value: ch },
                      { kind: 'bulk', value: message },
                    ],
                  })
                );
              } catch {
                // ignore write failures; socket cleanup unsubscribes
              }
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
        for (const ack of acks) state.socket.write(encodeReply(ack));
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
          state.socket.write(
            encodeReply({
              kind: 'array',
              items: [
                { kind: 'bulk', value: 'unsubscribe' },
                { kind: 'bulk', value: channel },
                { kind: 'int', value: state.subs.size },
              ],
            })
          );
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
            () => state.socket.write(encodeReply({ kind: 'simple', value: 'OK' })),
            err => state.socket.write(encodeReply({ kind: 'error', message: `ERR ${(err as Error).message}` }))
          )
          .catch(() => undefined);
        return 'silent';
      }

      case 'LOAD': {
        if (args.length > 1) throw new Error('LOAD takes an optional path');
        const path = args[0];
        this.load(path)
          .then(
            count => state.socket.write(encodeReply({ kind: 'simple', value: `OK ${count}` })),
            err => state.socket.write(encodeReply({ kind: 'error', message: `ERR ${(err as Error).message}` }))
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
    const n = Number(raw);
    if (!Number.isFinite(n) || n < 0) throw new Error(`TTL must be a finite number >= 0, got ${raw}`);
    return n;
  }
}

// Re-exported so `encodeCommand` users (tests, client) share one import root.
export { encodeCommand };
