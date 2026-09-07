// Standalone YASD cache server: one TCP port serves the RESP-like KV
// protocol and an HTTP `/healthz` endpoint (detected from the first bytes).
// Multi-instance Echo replicas point at it via YasdClient + CACHE_URL.
//
// Wire protocol (RESP2 subset, commands are arrays of bulk strings):
//   PING [msg] | GET key | SET key <json> [PX ms] | MSET k <json> ...
//   MGET k ... | DEL k ... | CLEAR prefix | TTL key | EXPIRE key ms
//   PERSIST key | INCR key [by] | DECR key [by]
//   PUBLISH channel msg | SUBSCRIBE ch ... | UNSUBSCRIBE [ch ...]
//   INFO | SAVE [path] | LOAD [path] | QUIT
// Cache values travel as JSON bulk strings (objects/arrays supported).
// Mutations are appended to the AOF (when configured) and published as
// invalidation events on `__yasd__:invalidate` for other replicas.

import * as net from 'net';
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
}

export interface ServerInfo {
  status: 'ok';
  version: string;
  uptimeMs: number;
  connections: number;
  entries: number;
  bytes: number;
  hits: number;
  misses: number;
  evictions: number;
  expiries: number;
  subscribers: number;
  channels: string[];
}

interface ConnState {
  socket: net.Socket;
  decoder: RespDecoder;
  httpBuf: Buffer | null; // non-null once HTTP detected
  subs: Map<string, PubSubListener>; // active subscriptions (empty = normal mode, null = never-subscribed?)
  subMode: boolean;
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
  if (env.YASD_LOAD_ON_START !== undefined) opts.loadOnStart = env.YASD_LOAD_ON_START !== '0';
  if (env.YASD_SAVE_ON_SHUTDOWN !== undefined) opts.saveOnShutdown = env.YASD_SAVE_ON_SHUTDOWN !== '0';
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
      entries: s.entries,
      bytes: s.bytes,
      hits: s.hits,
      misses: s.misses,
      evictions: s.evictions,
      expiries: s.expiries,
      subscribers: this.hub.subscriberCount(),
      channels: this.hub.channelNames(),
    };
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
    this.netServer = net.createServer(socket => this.onConnection(socket));
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
    if (state.subMode && cmd !== 'SUBSCRIBE' && cmd !== 'UNSUBSCRIBE' && cmd !== 'PING' && cmd !== 'QUIT') {
      state.socket.write(
        encodeReply({ kind: 'error', message: 'ERR only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT allowed in subscriber mode' })
      );
      return 'ok';
    }
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

  private dispatch(state: ConnState, cmd: string, args: string[]): RespReply | 'silent' | 'close' {
    switch (cmd) {
      case 'PING':
        return args.length > 0
          ? { kind: 'bulk', value: args.join(' ') }
          : { kind: 'simple', value: 'PONG' };

      case 'QUIT':
        return 'close';

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
