// Node client for the YASD cache server: pooled TCP connections speaking
// the RESP-like protocol, `CACHE_URL` config, pipelining-safe per-connection
// FIFO, healthcheck via HTTP `/healthz`, and multiplexed SUBSCRIBE.
//
//   const client = YasdClient.fromEnv(); // CACHE_URL=yasd://host:7379?poolSize=4
//   await client.connect();
//   await client.set('feeds:home', { posts: [] }, 15000);
//   const feed = await client.get('feeds:home');
//   const stop = await client.subscribe('presence', (ch, msg) => {...});
//   await client.close();

import * as net from 'net';
import * as http from 'http';
import { Value } from './types';
import { KVStats } from './cache';
import { RespDecoder, RespReply, encodeCommand } from './protocol';
import { DEFAULT_PORT, ServerInfo } from './server';

export interface YasdClientOptions {
  /** e.g. `yasd://127.0.0.1:7379?poolSize=4`. Host/port/poolSize fields win. */
  url?: string;
  host?: string;
  port?: number;
  /** Command-connection pool size. Default 4. */
  poolSize?: number;
  /** Per-request timeout in ms. Default 5000; 0 disables. */
  requestTimeoutMs?: number;
}

export interface ParsedCacheUrl {
  host: string;
  port: number;
  poolSize?: number;
}

export function parseCacheUrl(url: string): ParsedCacheUrl {
  const match = /^yasd:\/\/([^/:?#]+)(?::(\d+))?(?:\?(.*))?$/.exec(url.trim());
  if (!match) throw new Error(`invalid CACHE_URL (want yasd://host:port): ${url}`);
  const host = match[1] as string;
  const port = match[2] === undefined ? DEFAULT_PORT : parseInt(match[2], 10);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`invalid CACHE_URL port: ${url}`);
  }
  let poolSize: number | undefined;
  if (match[3]) {
    for (const pair of match[3].split('&')) {
      const [k, v] = pair.split('=');
      if (k === 'poolSize' && v !== undefined) poolSize = parseInt(v, 10);
    }
    if (poolSize !== undefined && (!Number.isInteger(poolSize) || poolSize < 1)) {
      throw new Error(`invalid CACHE_URL poolSize: ${url}`);
    }
  }
  return poolSize === undefined ? { host, port } : { host, port, poolSize };
}

type Pending = { resolve: (v: RespReply) => void; reject: (e: Error) => void; timer?: ReturnType<typeof setTimeout> };

interface PooledConn {
  socket: net.Socket;
  decoder: RespDecoder;
  pending: Pending[];
  dead: boolean;
}

export type SubscribeHandler = (channel: string, message: string) => void;

function jsValue(reply: RespReply): unknown {
  switch (reply.kind) {
    case 'simple':
      return reply.value;
    case 'error':
      throw new Error(reply.message.replace(/^ERR\s*/, ''));
    case 'int':
      return reply.value;
    case 'bulk':
      return reply.value;
    case 'array':
      return reply.items.map(item => (item === null ? null : jsValue(item)));
  }
}

export class YasdClient {
  private host: string;
  private port: number;
  private poolSize: number;
  private requestTimeoutMs: number;
  private pool: PooledConn[] = [];
  private roundRobin = 0;
  private closed = false;
  private connecting?: Promise<void>;
  // Shared subscriber connection (multiplexed channels).
  private subSocket?: net.Socket;
  private subDecoder = new RespDecoder();
  private subHandlers = new Map<string, Set<SubscribeHandler>>();
  private subConnecting?: Promise<void>;

  constructor(options: YasdClientOptions = {}) {
    const fromUrl = options.url ? parseCacheUrl(options.url) : undefined;
    this.host = options.host ?? fromUrl?.host ?? '127.0.0.1';
    this.port = options.port ?? fromUrl?.port ?? DEFAULT_PORT;
    this.poolSize = options.poolSize ?? fromUrl?.poolSize ?? 4;
    if (!Number.isInteger(this.poolSize) || this.poolSize < 1) {
      throw new Error('poolSize must be an integer >= 1');
    }
    this.requestTimeoutMs = options.requestTimeoutMs ?? 5000;
  }

  /** Build from `CACHE_URL` (falls back to localhost default when unset). */
  static fromEnv(env: NodeJS.ProcessEnv = process.env): YasdClient {
    return new YasdClient(env.CACHE_URL ? { url: env.CACHE_URL } : {});
  }

  get endpoint(): { host: string; port: number } {
    return { host: this.host, port: this.port };
  }

  async connect(): Promise<void> {
    if (this.closed) throw new Error('client is closed');
    if (this.connecting) {
      await this.connecting;
      return;
    }
    this.connecting = (async () => {
      const needed = this.poolSize - this.pool.filter(c => !c.dead).length;
      const created: PooledConn[] = [];
      try {
        for (let i = 0; i < needed; i++) {
          created.push(await this.dialCommand());
        }
      } catch (err) {
        for (const c of created) {
          try {
            c.socket.destroy();
          } catch {
            // ignore
          }
        }
        throw err;
      }
      this.pool.push(...created);
    })();
    try {
      await this.connecting;
    } finally {
      this.connecting = undefined;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.subSocket) {
      try {
        this.subSocket.destroy();
      } catch {
        // ignore
      }
      this.subSocket = undefined;
    }
    this.subHandlers.clear();
    this.subDecoder.reset();
    for (const conn of this.pool) {
      conn.dead = true;
      for (const p of conn.pending.splice(0)) {
        if (p.timer) clearTimeout(p.timer);
        p.reject(new Error('client is closed'));
      }
      try {
        conn.socket.destroy();
      } catch {
        // ignore
      }
    }
    this.pool = [];
  }

  // ---- health ----

  async ping(message?: string): Promise<string> {
    const reply = await this.exec(message === undefined ? ['PING'] : ['PING', message]);
    if (reply.kind === 'simple') return reply.value;
    if (reply.kind === 'bulk' && reply.value !== null) return reply.value;
    throw new Error(`unexpected PING reply: ${JSON.stringify(reply)}`);
  }

  /** HTTP `/healthz` against the server port. Throws when unhealthy. */
  async healthcheck(timeoutMs = 3000): Promise<ServerInfo> {
    return new Promise<ServerInfo>((resolve, reject) => {
      const req = http.get(
        { host: this.host, port: this.port, path: '/healthz', timeout: timeoutMs },
        res => {
          let body = '';
          res.on('data', chunk => {
            body += String(chunk);
          });
          res.on('end', () => {
            if (res.statusCode !== 200) {
              reject(new Error(`healthcheck failed: HTTP ${res.statusCode}`));
              return;
            }
            try {
              resolve(JSON.parse(body) as ServerInfo);
            } catch (err) {
              reject(err as Error);
            }
          });
        }
      );
      req.on('timeout', () => {
        req.destroy(new Error('healthcheck timed out'));
      });
      req.on('error', reject);
    });
  }

  // ---- KV ----

  /** Returns undefined on miss. Values round-trip as JSON. */
  async get(key: string): Promise<Value | undefined> {
    const reply = await this.exec(['GET', key]);
    if (reply.kind === 'bulk') {
      if (reply.value === null) return undefined;
      return JSON.parse(reply.value) as Value;
    }
    throw new Error(`unexpected GET reply: ${JSON.stringify(reply)}`);
  }

  async set(key: string, value: Value, ttlMs?: number): Promise<'OK'> {
    if (value === undefined) throw new Error('cannot cache undefined (use null)');
    const json = JSON.stringify(value);
    if (json === undefined) throw new Error('value is not JSON-serializable');
    const args = ttlMs === undefined ? ['SET', key, json] : ['SET', key, json, 'PX', String(ttlMs)];
    const reply = await this.exec(args);
    if (reply.kind === 'simple' && reply.value === 'OK') return 'OK';
    throw new Error(`unexpected SET reply: ${JSON.stringify(reply)}`);
  }

  async mget(keys: string[]): Promise<Array<Value | undefined>> {
    const reply = await this.exec(['MGET', ...keys]);
    if (reply.kind !== 'array') throw new Error(`unexpected MGET reply: ${JSON.stringify(reply)}`);
    return reply.items.map(item => {
      if (item === null) return undefined;
      if (item.kind === 'bulk') {
        return item.value === null ? undefined : (JSON.parse(item.value) as Value);
      }
      throw new Error(`unexpected MGET element: ${JSON.stringify(item)}`);
    });
  }

  async mset(entries: Array<{ key: string; value: Value }>): Promise<'OK'> {
    const args: string[] = ['MSET'];
    for (const e of entries) {
      if (e.value === undefined) throw new Error('cannot cache undefined (use null)');
      const json = JSON.stringify(e.value);
      if (json === undefined) throw new Error('value is not JSON-serializable');
      args.push(e.key, json);
    }
    const reply = await this.exec(args);
    if (reply.kind === 'simple' && reply.value === 'OK') return 'OK';
    throw new Error(`unexpected MSET reply: ${JSON.stringify(reply)}`);
  }

  async del(...keys: string[]): Promise<number> {
    return Number(await this.expectInt(['DEL', ...keys]));
  }

  async clearPrefix(prefix: string): Promise<number> {
    return Number(await this.expectInt(['CLEAR', prefix]));
  }

  /** Ms remaining; -1 = persists; -2 = missing/expired. */
  async ttl(key: string): Promise<number> {
    return Number(await this.expectInt(['TTL', key]));
  }

  /** Returns false when missing/expired. */
  async expire(key: string, ttlMs: number): Promise<boolean> {
    return (await this.expectInt(['EXPIRE', key, String(ttlMs)])) === 1;
  }

  /** Returns false when missing/expired. */
  async persist(key: string): Promise<boolean> {
    return (await this.expectInt(['PERSIST', key])) === 1;
  }

  async incr(key: string, by = 1): Promise<number> {
    return this.numberReply(await this.exec(['INCR', key, String(by)]));
  }

  async decr(key: string, by = 1): Promise<number> {
    return this.numberReply(await this.exec(['DECR', key, String(by)]));
  }

  async publish(channel: string, message: string): Promise<number> {
    return Number(await this.expectInt(['PUBLISH', channel, message]));
  }

  async info(): Promise<ServerInfo & { stats?: KVStats }> {
    const reply = await this.exec(['INFO']);
    if (reply.kind === 'bulk' && reply.value !== null) {
      return JSON.parse(reply.value) as ServerInfo;
    }
    throw new Error(`unexpected INFO reply: ${JSON.stringify(reply)}`);
  }

  async save(path?: string): Promise<'OK'> {
    const reply = await this.exec(path === undefined ? ['SAVE'] : ['SAVE', path]);
    if (reply.kind === 'simple' && reply.value === 'OK') return 'OK';
    throw new Error(`unexpected SAVE reply: ${JSON.stringify(reply)}`);
  }

  async load(path?: string): Promise<string> {
    const reply = await this.exec(path === undefined ? ['LOAD'] : ['LOAD', path]);
    if (reply.kind === 'simple') return reply.value;
    throw new Error(`unexpected LOAD reply: ${JSON.stringify(reply)}`);
  }

  // ---- pub/sub (multiplexed over one dedicated connection) ----

  /**
   * Subscribe to a channel. Returns an unsubscribe function. The SUBSCRIBE
   * round trip completes before this resolves, so no message is missed after.
   */
  async subscribe(channel: string, handler: SubscribeHandler): Promise<() => Promise<void>> {
    if (!channel) throw new Error('subscribe requires a channel');
    if (typeof handler !== 'function') throw new Error('subscribe requires a handler');
    await this.ensureSubConn();
    let set = this.subHandlers.get(channel);
    if (!set) {
      set = new Set();
      this.subHandlers.set(channel, set);
    }
    const first = set.size === 0 && !this.subPendingAcks.has(channel);
    set.add(handler);
    if (first) {
      await this.subRoundTrip(['SUBSCRIBE', channel]);
    }
    let unsubscribed = false;
    return async () => {
      if (unsubscribed) return;
      unsubscribed = true;
      set?.delete(handler);
      if (set && set.size === 0) {
        this.subHandlers.delete(channel);
        if (this.subSocket) {
          try {
            await this.subRoundTrip(['UNSUBSCRIBE', channel]);
          } catch {
            // connection may already be gone; handlers are already dropped
          }
        }
      }
      if (this.subHandlers.size === 0 && this.subSocket) {
        try {
          this.subSocket.destroy();
        } catch {
          // ignore
        }
        this.subSocket = undefined;
        this.subDecoder.reset();
      }
    };
  }

  // ---- internals ----

  private async expectInt(cmd: string[]): Promise<number> {
    const v = jsValue(await this.exec(cmd));
    if (typeof v !== 'number') throw new Error(`unexpected integer reply for ${cmd[0]}`);
    return v;
  }

  private numberReply(reply: RespReply): number {
    const v = jsValue(reply);
    if (typeof v === 'number') return v;
    if (typeof v === 'string') {
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
    throw new Error(`unexpected numeric reply: ${JSON.stringify(reply)}`);
  }

  private async exec(cmd: string[]): Promise<RespReply> {
    if (this.closed) throw new Error('client is closed');
    const conn = await this.liveConn();
    return new Promise<RespReply>((resolve, reject) => {
      const pending: Pending = { resolve, reject };
      if (this.requestTimeoutMs > 0) {
        pending.timer = setTimeout(() => {
          const i = conn.pending.indexOf(pending);
          if (i !== -1) this.failConn(conn, new Error(`request timed out: ${cmd[0]}`));
          reject(new Error(`request timed out: ${cmd[0]}`));
        }, this.requestTimeoutMs);
        const t = pending.timer as unknown as { unref?: () => void };
        if (typeof t.unref === 'function') t.unref();
      }
      conn.pending.push(pending);
      try {
        conn.socket.write(encodeCommand(cmd));
      } catch (err) {
        conn.pending.pop();
        if (pending.timer) clearTimeout(pending.timer);
        this.failConn(conn, err as Error);
        reject(err as Error);
      }
    });
  }

  private async liveConn(): Promise<PooledConn> {
    await this.connect();
    for (let i = 0; i < this.pool.length; i++) {
      this.roundRobin = (this.roundRobin + 1) % this.pool.length;
      const conn = this.pool[this.roundRobin] as PooledConn;
      if (!conn.dead) return conn;
    }
    // All dead (or pool emptied): reconnect fresh.
    this.pool = [];
    await this.connect();
    const conn = this.pool[0];
    if (!conn) throw new Error('no connections available');
    return conn;
  }

  private dialCommand(): Promise<PooledConn> {
    return new Promise<PooledConn>((resolve, reject) => {
      const socket = net.createConnection({ host: this.host, port: this.port });
      const conn: PooledConn = { socket, decoder: new RespDecoder(), pending: [], dead: false };
      const onError = (err: Error): void => {
        socket.off('connect', onConnect);
        reject(err);
      };
      const onConnect = (): void => {
        socket.off('error', onError);
        socket.on('error', () => this.failConn(conn, new Error('connection error')));
        resolve(conn);
      };
      socket.once('error', onError);
      socket.once('connect', onConnect);
      socket.on('data', chunk => {
        let replies: RespReply[];
        try {
          replies = conn.decoder.push(Buffer.from(chunk));
        } catch (err) {
          this.failConn(conn, err as Error);
          return;
        }
        for (const reply of replies) {
          const pending = conn.pending.shift();
          if (!pending) continue; // stray reply (e.g. after timeout teardown)
          if (pending.timer) clearTimeout(pending.timer);
          if (reply.kind === 'error') {
            pending.reject(new Error(reply.message.replace(/^ERR\s*/, '')));
          } else {
            pending.resolve(reply);
          }
        }
      });
      socket.on('close', () => this.failConn(conn, new Error('connection closed')));
    });
  }

  private failConn(conn: PooledConn, err: Error): void {
    if (conn.dead) return;
    conn.dead = true;
    for (const p of conn.pending.splice(0)) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(err);
    }
    try {
      conn.socket.destroy();
    } catch {
      // ignore
    }
  }

  // -- subscriber connection --

  private subPendingAcks = new Set<string>();
  private subAckWaiters = new Map<string, Array<{ resolve: () => void; reject: (e: Error) => void }>>();

  private async ensureSubConn(): Promise<void> {
    if (this.closed) throw new Error('client is closed');
    if (this.subSocket) return;
    if (this.subConnecting) {
      await this.subConnecting;
      return;
    }
    this.subConnecting = (async () => {
      // Re-register surviving handlers after a reconnect.
      const channels = Array.from(this.subHandlers.keys());
      await new Promise<void>((resolve, reject) => {
        const socket = net.createConnection({ host: this.host, port: this.port });
        const onError = (err: Error): void => {
          socket.off('connect', onConnect);
          reject(err);
        };
        const onConnect = (): void => {
          socket.off('error', onError);
          this.subSocket = socket;
          this.subDecoder.reset();
          socket.on('data', chunk => this.onSubData(Buffer.from(chunk)));
          socket.on('error', () => this.onSubLost());
          socket.on('close', () => this.onSubLost());
          resolve();
        };
        socket.once('error', onError);
        socket.once('connect', onConnect);
      });
      for (const channel of channels) {
        this.subPendingAcks.add(channel);
        this.subSocket?.write(encodeCommand(['SUBSCRIBE', channel]));
      }
      // New subscriptions resolve via their own acks; re-subscribes here are
      // fire-and-retry on next publish-independent basis — wait briefly for
      // acks so ordering holds, but don't hang close().
      await this.waitSubAcks(channels, 3000).catch(() => undefined);
    })();
    try {
      await this.subConnecting;
    } finally {
      this.subConnecting = undefined;
    }
  }

  private onSubLost(): void {
    this.subSocket = undefined;
    this.subDecoder.reset();
    for (const waiters of this.subAckWaiters.values()) {
      for (const w of waiters.splice(0)) w.reject(new Error('subscriber connection lost'));
    }
    this.subAckWaiters.clear();
    this.subPendingAcks.clear();
  }

  private onSubData(chunk: Buffer): void {
    let replies: RespReply[];
    try {
      replies = this.subDecoder.push(chunk);
    } catch {
      this.onSubLost();
      return;
    }
    for (const reply of replies) {
      if (reply.kind !== 'array') continue;
      const parts = reply.items.map(item => (item && item.kind === 'bulk' ? item.value : null));
      const [kind, channel, payload] = parts;
      if (kind === 'subscribe' && typeof channel === 'string') {
        this.subPendingAcks.delete(channel);
        const waiters = this.subAckWaiters.get(channel);
        if (waiters) {
          this.subAckWaiters.delete(channel);
          for (const w of waiters.splice(0)) w.resolve();
        }
      } else if (kind === 'message' && typeof channel === 'string' && typeof payload === 'string') {
        const handlers = this.subHandlers.get(channel);
        if (handlers) {
          for (const h of Array.from(handlers)) {
            try {
              h(channel, payload);
            } catch {
              // never break delivery
            }
          }
        }
      }
      // unsubscribe acks need no client-side action
    }
  }

  private subRoundTrip(cmd: string[]): Promise<void> {
    const channel = cmd[1] as string;
    return new Promise<void>((resolve, reject) => {
      let waiters = this.subAckWaiters.get(channel);
      if (!waiters) {
        waiters = [];
        this.subAckWaiters.set(channel, waiters);
      }
      waiters.push({ resolve, reject });
      this.subPendingAcks.add(channel);
      try {
        this.subSocket?.write(encodeCommand(cmd));
      } catch (err) {
        reject(err as Error);
      }
      if (this.requestTimeoutMs > 0) {
        const timer = setTimeout(() => {
          const ws = this.subAckWaiters.get(channel);
          if (ws) {
            const i = ws.findIndex(w => w.resolve === resolve);
            if (i !== -1) {
              ws.splice(i, 1);
              reject(new Error(`subscribe timed out: ${channel}`));
            }
          }
        }, this.requestTimeoutMs);
        const t = timer as unknown as { unref?: () => void };
        if (typeof t.unref === 'function') t.unref();
      }
    });
  }

  private waitSubAcks(channels: string[], timeoutMs: number): Promise<void> {
    const pending = channels.filter(c => this.subPendingAcks.has(c));
    if (pending.length === 0) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('resubscribe timed out')), timeoutMs);
      const t = timer as unknown as { unref?: () => void };
      if (typeof t.unref === 'function') t.unref();
      const check = (): void => {
        if (pending.every(c => !this.subPendingAcks.has(c))) {
          clearTimeout(timer);
          resolve();
        } else {
          setTimeout(check, 25).unref?.();
        }
      };
      check();
    });
  }
}
