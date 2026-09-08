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
import * as tls from 'tls';
import * as http from 'http';
import { Value, JsonValue } from './types';
import { KVStats, TransactionError } from './cache';
import { RespDecoder, RespReply, encodeCommand } from './protocol';
import { DEFAULT_PORT, ServerInfo } from './server';

export interface YasdClientOptions {
  /** e.g. `yasd://127.0.0.1:7379?poolSize=4` (`yasds://` enables TLS). Host/port/poolSize fields win. */
  url?: string;
  host?: string;
  port?: number;
  /** Command-connection pool size. Default 4. */
  poolSize?: number;
  /** Per-request timeout in ms. Default 5000; 0 disables. */
  requestTimeoutMs?: number;
  /** Server password (AUTH). Also read from CACHE_URL userinfo/query. */
  password?: string;
  /** TLS: `true` for defaults, or `tls.ConnectionOptions` (ca, rejectUnauthorized, ...). */
  tls?: boolean | tls.ConnectionOptions;
}

export interface ParsedCacheUrl {
  host: string;
  port: number;
  poolSize?: number;
  password?: string;
  tls?: boolean;
}

export function parseCacheUrl(url: string): ParsedCacheUrl {
  const trimmed = url.trim();
  // yasd://[[user]:password@]host[:port][?poolSize=&password=] — yasds:// enables TLS.
  const match = /^(yasds?):\/\/(?:([^@/?#]*)@)?([^/:?#]+)(?::(\d+))?(?:\?(.*))?$/.exec(trimmed);
  if (!match) throw new Error(`invalid CACHE_URL (want yasd://host:port): ${url}`);
  const tls = match[1] === 'yasds';
  const userinfo = match[2];
  const host = match[3] as string;
  const port = match[4] === undefined ? DEFAULT_PORT : parseInt(match[4], 10);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`invalid CACHE_URL port: ${url}`);
  }
  let poolSize: number | undefined;
  let password: string | undefined;
  if (userinfo !== undefined && userinfo.length > 0) {
    const colon = userinfo.indexOf(':');
    password = decodeURIComponent(colon === -1 ? userinfo : userinfo.slice(colon + 1));
    if (password.length === 0) password = undefined;
  }
  if (match[5]) {
    for (const pair of match[5].split('&')) {
      const [k, v] = pair.split('=');
      if (k === 'poolSize' && v !== undefined) poolSize = parseInt(v, 10);
      if (k === 'password' && v !== undefined) password = decodeURIComponent(v);
    }
    if (poolSize !== undefined && (!Number.isInteger(poolSize) || poolSize < 1)) {
      throw new Error(`invalid CACHE_URL poolSize: ${url}`);
    }
  }
  const out: ParsedCacheUrl = { host, port };
  if (poolSize !== undefined) out.poolSize = poolSize;
  if (password !== undefined) out.password = password;
  if (tls) out.tls = true;
  return out;
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
    case 'nil':
      return null;
  }
}

export class YasdClient {
  private host: string;
  private port: number;
  private poolSize: number;
  private requestTimeoutMs: number;
  private password: string | undefined;
  private tlsOptions: tls.ConnectionOptions | undefined;
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
    const password = options.password ?? fromUrl?.password;
    this.password = password && password.length > 0 ? password : undefined;
    if (options.tls !== undefined) {
      this.tlsOptions = options.tls === true ? {} : { ...options.tls };
    } else if (fromUrl?.tls) {
      this.tlsOptions = {};
    }
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

  /**
   * Compare-and-set. `expected === undefined` asserts absence (send as an
   * empty arg — it is never valid JSON, so no value collides with it).
   * Returns true on success, false when the live value differs.
   */
  async cas(key: string, expected: Value | undefined, value: Value, ttlMs?: number): Promise<boolean> {
    if (value === undefined) throw new Error('cannot cache undefined (use null)');
    const valueJson = JSON.stringify(value);
    if (valueJson === undefined) throw new Error('value is not JSON-serializable');
    const expectedArg = expected === undefined ? '' : JSON.stringify(expected);
    if (expectedArg === undefined) throw new Error('expected is not JSON-serializable');
    const args =
      ttlMs === undefined
        ? ['CAS', key, expectedArg, valueJson]
        : ['CAS', key, expectedArg, valueJson, 'PX', String(ttlMs)];
    const reply = await this.exec(args);
    if (reply.kind === 'int') return reply.value === 1;
    throw new Error(`unexpected CAS reply: ${JSON.stringify(reply)}`);
  }

  async publish(channel: string, message: string): Promise<number> {
    return Number(await this.expectInt(['PUBLISH', channel, message]));
  }

  // ---- transactions (multi-key read-modify-write) ----

  /**
   * Start an optimistic transaction on a dedicated connection (WATCH/MULTI/
   * EXEC must share one connection, so transactions bypass the pool):
   * ```typescript
   * const tx = client.multi();
   * await tx.watch('likes:1');
   * const cur = (await tx.get('likes:1') as number) ?? 0;
   * await tx.set('likes:1', cur + 1); // first write auto-sends MULTI
   * const results = await tx.exec();  // null = conflict, retry
   * ```
   * One-shot: `exec()`/`discard()`/`close()` finish the transaction.
   */
  multi(): YasdTransaction {
    if (this.closed) throw new Error('client is closed');
    return new YasdTransaction({
      host: this.host,
      port: this.port,
      password: this.password,
      tlsOptions: this.tlsOptions,
      requestTimeoutMs: this.requestTimeoutMs,
    });
  }

  /**
   * Watch-then-commit loop with retries. `fn` reads via `tx.get/mget/ttl`
   * and queues writes via `tx.set/del/incr/...` (first write auto-sends
   * MULTI); the commit retries on conflict up to `maxRetries` times. A throw
   * inside `fn` discards and rethrows; `tx.discard()` inside `fn` aborts
   * voluntarily (`committed: false`, no retry).
   */
  async runTransaction<T>(
    keys: string[],
    fn: (tx: YasdTransaction) => T | Promise<T>,
    maxRetries = 3
  ): Promise<{ committed: boolean; attempts: number; results: TxExecResult[] | null; value: T | undefined }> {
    if (!Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new Error('maxRetries must be an integer >= 0');
    }
    let attempts = 0;
    let value: T | undefined;
    for (;;) {
      const tx = this.multi();
      attempts++;
      try {
        await tx.watch(...keys);
        value = await fn(tx);
        if (tx.finished) {
          return { committed: false, attempts, results: null, value };
        }
        const results = await tx.exec();
        if (results !== null) {
          return { committed: true, attempts, results, value };
        }
      } catch (err) {
        await tx.close().catch(() => undefined);
        throw err;
      }
      if (attempts > maxRetries) {
        return { committed: false, attempts, results: null, value };
      }
    }
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
    return this.sendOn(conn, cmd);
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

  /** Open one transport socket (TLS when configured), resolved when ready. */
  private dialRaw(): Promise<net.Socket> {
    return new Promise<net.Socket>((resolve, reject) => {
      const readyEvent = this.tlsOptions !== undefined ? 'secureConnect' : 'connect';
      let socket: net.Socket;
      try {
        socket = this.tlsOptions !== undefined
          ? tls.connect({ host: this.host, port: this.port, ...this.tlsOptions })
          : net.createConnection({ host: this.host, port: this.port });
      } catch (err) {
        reject(err as Error);
        return;
      }
      const onError = (err: Error): void => {
        socket.off(readyEvent, onReady);
        reject(err);
      };
      const onReady = (): void => {
        socket.off('error', onError);
        resolve(socket);
      };
      socket.once('error', onError);
      socket.once(readyEvent, onReady);
    });
  }

  private attachCommandHandlers(conn: PooledConn): void {
    const { socket } = conn;
    socket.on('error', () => this.failConn(conn, new Error('connection error')));
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
  }

  /** Send a command on an explicit connection (used by exec + AUTH on dial). */
  private sendOn(conn: PooledConn, cmd: string[]): Promise<RespReply> {
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

  private async dialCommand(): Promise<PooledConn> {
    const socket = await this.dialRaw();
    const conn: PooledConn = { socket, decoder: new RespDecoder(), pending: [], dead: false };
    this.attachCommandHandlers(conn);
    if (this.password !== undefined) {
      try {
        const reply = await this.sendOn(conn, ['AUTH', this.password]);
        if (reply.kind !== 'simple' || reply.value !== 'OK') {
          throw new Error('authentication failed');
        }
      } catch (err) {
        this.failConn(conn, err as Error);
        throw err;
      }
    }
    return conn;
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
      const socket = await this.dialRaw();
      if (this.password !== undefined) {
        const decoder = new RespDecoder();
        socket.write(encodeCommand(['AUTH', this.password]));
        const authed = await new Promise<boolean>(resolve => {
          const onData = (chunk: Buffer): void => {
            const replies = decoder.push(chunk);
            if (replies.length > 0) {
              const first = replies[0] as RespReply;
              socket.off('data', onData);
              resolve(first.kind === 'simple' && first.value === 'OK');
            }
          };
          socket.on('data', onData);
          socket.once('error', () => resolve(false));
          socket.once('close', () => resolve(false));
        });
        if (!authed) {
          try {
            socket.destroy();
          } catch {
            // ignore
          }
          throw new Error('authentication failed');
        }
      }
      this.subSocket = socket;
      this.subDecoder.reset();
      socket.on('data', chunk => this.onSubData(Buffer.from(chunk)));
      socket.on('error', () => this.onSubLost());
      socket.on('close', () => this.onSubLost());
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

/** Decoded EXEC per-op value (bulk values are JSON-parsed like GET/MGET). */
export type TxExecResult = JsonValue | undefined;

function txValue(reply: RespReply): TxExecResult {
  switch (reply.kind) {
    case 'simple':
      return reply.value;
    case 'int':
      return reply.value;
    case 'bulk': {
      if (reply.value === null) return undefined;
      try {
        return JSON.parse(reply.value) as JsonValue;
      } catch {
        return reply.value; // e.g. PING echo inside MULTI (raw, not JSON)
      }
    }
    case 'array':
      return reply.items.map(item => (item === null ? null : txValue(item))) as JsonValue;
    case 'nil':
      return null;
    case 'error':
      throw new Error(reply.message.replace(/^ERR\s*/, ''));
  }
}

export interface YasdTransactionOptions {
  host: string;
  port: number;
  password?: string;
  tlsOptions?: tls.ConnectionOptions;
  requestTimeoutMs?: number;
}

/**
 * Optimistic transaction over a dedicated connection (one-shot).
 * Reads execute immediately; the first queued write auto-sends MULTI and
 * `exec()` commits atomically (array of per-op results) or aborts with null
 * when a watched key changed. Reads after MULTI are rejected client-side —
 * read first, then write (the WATCH/read/MULTI/write/EXEC pattern).
 */
export class YasdTransaction {
  private host: string;
  private port: number;
  private password: string | undefined;
  private tlsOptions: tls.ConnectionOptions | undefined;
  private requestTimeoutMs: number;
  private socket?: net.Socket;
  private decoder = new RespDecoder();
  private pending: Pending[] = [];
  private connecting?: Promise<void>;
  private begun = false;
  private done = false;
  private dead = false;

  constructor(options: YasdTransactionOptions) {
    this.host = options.host;
    this.port = options.port;
    this.password = options.password;
    this.tlsOptions = options.tlsOptions;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 5000;
  }

  /** True once `exec()`, `discard()`, or `close()` has run. */
  get finished(): boolean {
    return this.done;
  }

  /** True once MULTI has been sent (first queued write auto-sends it). */
  get inMulti(): boolean {
    return this.begun;
  }

  async connect(): Promise<void> {
    if (this.done) throw new TransactionError('transaction is finished');
    if (this.socket) return;
    if (this.connecting) {
      await this.connecting;
      return;
    }
    this.connecting = (async () => {
      const socket: net.Socket = await new Promise<net.Socket>((resolve, reject) => {
        const readyEvent = this.tlsOptions !== undefined ? 'secureConnect' : 'connect';
        let s: net.Socket;
        try {
          s = this.tlsOptions !== undefined
            ? tls.connect({ host: this.host, port: this.port, ...this.tlsOptions })
            : net.createConnection({ host: this.host, port: this.port });
        } catch (err) {
          reject(err as Error);
          return;
        }
        const onError = (err: Error): void => {
          s.off(readyEvent, onReady);
          reject(err);
        };
        const onReady = (): void => {
          s.off('error', onError);
          resolve(s);
        };
        s.once('error', onError);
        s.once(readyEvent, onReady);
      });
      socket.on('data', chunk => this.onData(Buffer.from(chunk)));
      socket.on('error', () => this.fail(new Error('transaction connection error')));
      socket.on('close', () => this.fail(new Error('transaction connection closed')));
      this.socket = socket;
      if (this.password !== undefined) {
        const reply = await this.send(['AUTH', this.password]);
        if (reply.kind !== 'simple' || reply.value !== 'OK') {
          throw new Error('authentication failed');
        }
      }
    })();
    try {
      await this.connecting;
    } catch (err) {
      this.fail(err as Error);
      throw err;
    } finally {
      this.connecting = undefined;
    }
  }

  /** Snapshot versions of `keys`; aborts `exec()` if any change since. */
  async watch(...keys: string[]): Promise<'OK'> {
    this.assertWritable('WATCH');
    if (this.begun) throw new TransactionError('WATCH inside MULTI is not allowed');
    if (keys.length === 0) throw new TransactionError('WATCH requires at least one key');
    return this.expectOk(['WATCH', ...keys]);
  }

  /** Forget watched versions (queued writes are kept). */
  async unwatch(): Promise<'OK'> {
    this.assertWritable('UNWATCH');
    return this.expectOk(['UNWATCH']);
  }

  /** Immediate read (must precede MULTI — read first, then write). */
  async get(key: string): Promise<Value | undefined> {
    this.assertReadable('GET');
    const reply = await this.send(['GET', key]);
    if (reply.kind === 'bulk') {
      return reply.value === null ? undefined : (JSON.parse(reply.value) as Value);
    }
    throw new Error(`unexpected GET reply: ${JSON.stringify(reply)}`);
  }

  /** Immediate batch read (must precede MULTI). */
  async mget(keys: string[]): Promise<Array<Value | undefined>> {
    this.assertReadable('MGET');
    const reply = await this.send(['MGET', ...keys]);
    if (reply.kind !== 'array') throw new Error(`unexpected MGET reply: ${JSON.stringify(reply)}`);
    return reply.items.map(item => {
      if (item === null) return undefined;
      if (item.kind === 'bulk') {
        return item.value === null ? undefined : (JSON.parse(item.value) as Value);
      }
      throw new Error(`unexpected MGET element: ${JSON.stringify(item)}`);
    });
  }

  /** Immediate TTL read (must precede MULTI). */
  async ttl(key: string): Promise<number> {
    this.assertReadable('TTL');
    const reply = await this.send(['TTL', key]);
    if (reply.kind === 'int') return reply.value;
    throw new Error(`unexpected TTL reply: ${JSON.stringify(reply)}`);
  }

  /** Queue a SET (first write auto-sends MULTI). */
  async set(key: string, value: Value, ttlMs?: number): Promise<void> {
    if (value === undefined) throw new TransactionError('cannot cache undefined (use null)');
    const json = JSON.stringify(value);
    if (json === undefined) throw new TransactionError('value is not JSON-serializable');
    await this.queue(
      ttlMs === undefined ? ['SET', key, json] : ['SET', key, json, 'PX', String(ttlMs)]
    );
  }

  /** Queue an MSET batch. */
  async mset(entries: Array<{ key: string; value: Value }>): Promise<void> {
    const args: string[] = ['MSET'];
    for (const e of entries) {
      if (e.value === undefined) throw new TransactionError('cannot cache undefined (use null)');
      const json = JSON.stringify(e.value);
      if (json === undefined) throw new TransactionError('value is not JSON-serializable');
      args.push(e.key, json);
    }
    await this.queue(args);
  }

  /** Queue a DEL. */
  async del(...keys: string[]): Promise<void> {
    await this.queue(['DEL', ...keys]);
  }

  /** Queue a namespace clear. */
  async clearPrefix(prefix: string): Promise<void> {
    await this.queue(['CLEAR', prefix]);
  }

  /** Queue an EXPIRE. */
  async expire(key: string, ttlMs: number): Promise<void> {
    await this.queue(['EXPIRE', key, String(ttlMs)]);
  }

  /** Queue a PERSIST. */
  async persist(key: string): Promise<void> {
    await this.queue(['PERSIST', key]);
  }

  /** Queue an INCR. */
  async incr(key: string, by = 1): Promise<void> {
    await this.queue(['INCR', key, String(by)]);
  }

  /** Queue a DECR. */
  async decr(key: string, by = 1): Promise<void> {
    await this.queue(['DECR', key, String(by)]);
  }

  /** Queue a CAS (`expected === undefined` asserts absence). */
  async cas(key: string, expected: Value | undefined, value: Value, ttlMs?: number): Promise<void> {
    if (value === undefined) throw new TransactionError('cannot cache undefined (use null)');
    const valueJson = JSON.stringify(value);
    if (valueJson === undefined) throw new TransactionError('value is not JSON-serializable');
    const expectedArg = expected === undefined ? '' : JSON.stringify(expected);
    if (expectedArg === undefined) throw new TransactionError('expected is not JSON-serializable');
    await this.queue(
      ttlMs === undefined
        ? ['CAS', key, expectedArg, valueJson]
        : ['CAS', key, expectedArg, valueJson, 'PX', String(ttlMs)]
    );
  }

  /** Send MULTI explicitly (optional — the first write auto-sends it). */
  async begin(): Promise<'OK'> {
    this.assertWritable('MULTI');
    if (this.begun) throw new TransactionError('MULTI calls cannot nest');
    const ok = await this.expectOk(['MULTI']);
    this.begun = true;
    return ok;
  }

  /**
   * Commit: array of per-op results in queue order, or null when a watched
   * key changed (nothing applied). A per-op runtime failure throws (earlier
   * ops in the batch were already applied server-side).
   */
  async exec(): Promise<TxExecResult[] | null> {
    this.assertWritable('EXEC');
    this.done = true;
    try {
      if (!this.begun) {
        const ok = await this.expectOk(['MULTI']);
        if (ok !== 'OK') throw new Error('MULTI failed');
        this.begun = true;
      }
      const reply = await this.send(['EXEC']);
      if (reply.kind === 'nil') return null;
      if (reply.kind !== 'array') throw new Error(`unexpected EXEC reply: ${JSON.stringify(reply)}`);
      return reply.items.map(item => (item === null ? null : txValue(item)));
    } finally {
      await this.closeSocket();
    }
  }

  /** Drop the queue (and watches) without committing. */
  async discard(): Promise<void> {
    this.assertWritable('DISCARD');
    this.done = true;
    try {
      if (this.begun) await this.send(['DISCARD']);
    } finally {
      await this.closeSocket();
    }
  }

  /** Abandon the transaction (watches die with the connection). */
  async close(): Promise<void> {
    this.done = true;
    await this.closeSocket();
  }

  // ---- internals ----

  private assertWritable(what: string): void {
    if (this.done) throw new TransactionError(`cannot ${what}: transaction is finished`);
  }

  private assertReadable(what: string): void {
    this.assertWritable(what);
    if (this.begun) {
      throw new TransactionError(`${what} must precede MULTI (read first, then write)`);
    }
  }

  /** Queue a write: auto-sends MULTI on the first write, expects QUEUED. */
  private async queue(cmd: string[]): Promise<void> {
    this.assertWritable(cmd[0] ?? 'queue');
    await this.connect();
    if (!this.begun) await this.begin();
    const reply = await this.send(cmd);
    if (reply.kind !== 'simple' || reply.value !== 'QUEUED') {
      throw new Error(`unexpected ${cmd[0]} reply inside MULTI: ${JSON.stringify(reply)}`);
    }
  }

  private async expectOk(cmd: string[]): Promise<'OK'> {
    await this.connect();
    const reply = await this.send(cmd);
    if (reply.kind === 'simple' && reply.value === 'OK') return 'OK';
    throw new Error(`unexpected ${cmd[0]} reply: ${JSON.stringify(reply)}`);
  }

  private send(cmd: string[]): Promise<RespReply> {
    const socket = this.socket;
    if (!socket || this.dead) return Promise.reject(new Error('transaction connection is closed'));
    return new Promise<RespReply>((resolve, reject) => {
      const pending: Pending = { resolve, reject };
      if (this.requestTimeoutMs > 0) {
        pending.timer = setTimeout(() => {
          this.fail(new Error(`request timed out: ${cmd[0]}`));
          reject(new Error(`request timed out: ${cmd[0]}`));
        }, this.requestTimeoutMs);
        const t = pending.timer as unknown as { unref?: () => void };
        if (typeof t.unref === 'function') t.unref();
      }
      this.pending.push(pending);
      try {
        socket.write(encodeCommand(cmd));
      } catch (err) {
        this.pending.pop();
        if (pending.timer) clearTimeout(pending.timer);
        this.fail(err as Error);
        reject(err as Error);
      }
    });
  }

  private onData(chunk: Buffer): void {
    let replies: RespReply[];
    try {
      replies = this.decoder.push(chunk);
    } catch (err) {
      this.fail(err as Error);
      return;
    }
    for (const reply of replies) {
      const pending = this.pending.shift();
      if (!pending) continue;
      if (pending.timer) clearTimeout(pending.timer);
      if (reply.kind === 'error') {
        pending.reject(new Error(reply.message.replace(/^ERR\s*/, '')));
      } else {
        pending.resolve(reply);
      }
    }
  }

  private fail(err: Error): void {
    if (this.dead) return;
    this.dead = true;
    for (const p of this.pending.splice(0)) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(err);
    }
    try {
      this.socket?.destroy();
    } catch {
      // ignore
    }
    this.socket = undefined;
  }

  private async closeSocket(): Promise<void> {
    this.dead = true;
    for (const p of this.pending.splice(0)) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(new Error('transaction is finished'));
    }
    const socket = this.socket;
    this.socket = undefined;
    if (socket) {
      try {
        socket.destroy();
      } catch {
        // ignore
      }
    }
  }
}
