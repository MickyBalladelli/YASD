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
import * as https from 'https';
import { Value, JsonValue } from './types';
import { KVStats, TransactionError } from './cache';
import { RespDecoder, RespReply, encodeCommand } from './protocol';
import { DEFAULT_PORT, ServerInfo } from './server';
import {
  parsePort,
  parseStrictInteger,
  validateHost,
  validateNonNegativeSafeInteger,
  validateNonNegativeNumber,
  validatePort,
  validatePositiveSafeInteger,
  validateTimeout,
} from './validation';

const RECONNECT_MAX_ATTEMPTS = 4;
const RECONNECT_BASE_DELAY_MS = 25;
const RECONNECT_MAX_DELAY_MS = 1000;
const MAX_CLIENT_POOL_SIZE = 1024;

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

function decodeUrlPart(value: string, url: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new Error(`invalid CACHE_URL encoding: ${url}`);
  }
}

function validatePassword(value: unknown, name: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string') throw new Error(`${name} must be a string`);
  return value.length === 0 ? undefined : value;
}

function resolveTlsOptions(
  value: unknown,
  urlTls: boolean | undefined,
  name = 'tls'
): tls.ConnectionOptions | undefined {
  if (value === undefined) return urlTls === true ? {} : undefined;
  if (typeof value === 'boolean') {
    if (urlTls !== undefined && value !== urlTls) {
      throw new Error(`${name} conflicts with the URL TLS scheme`);
    }
    return value ? {} : undefined;
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${name} must be true, false, or a TLS options object`);
  }
  if (urlTls === false) throw new Error(`${name} conflicts with the URL TLS scheme`);
  const tlsOptions = value as tls.ConnectionOptions;
  const hasKey = tlsOptions.key !== undefined;
  const hasCert = tlsOptions.cert !== undefined;
  if (hasKey !== hasCert && tlsOptions.pfx === undefined) {
    throw new Error(`${name} requires both key and cert, or pfx`);
  }
  return { ...tlsOptions };
}

interface ResolvedClientOptions {
  host: string;
  port: number;
  poolSize: number;
  requestTimeoutMs: number;
  password: string | undefined;
  tlsOptions: tls.ConnectionOptions | undefined;
}

function resolveClientOptions(options: YasdClientOptions): ResolvedClientOptions {
  const fromUrl = options.url === undefined ? undefined : parseCacheUrl(options.url);
  const poolSize = options.poolSize === undefined
    ? fromUrl?.poolSize ?? 4
    : validatePositiveSafeInteger(options.poolSize, 'poolSize', MAX_CLIENT_POOL_SIZE);
  return {
    host: validateHost(
      options.host === undefined ? fromUrl?.host ?? '127.0.0.1' : options.host,
      'host'
    ),
    port: options.port === undefined
      ? fromUrl?.port ?? DEFAULT_PORT
      : validatePort(options.port, 'port'),
    poolSize: validatePositiveSafeInteger(poolSize, 'poolSize', MAX_CLIENT_POOL_SIZE),
    requestTimeoutMs: options.requestTimeoutMs === undefined
      ? 5000
      : validateTimeout(options.requestTimeoutMs, 'requestTimeoutMs'),
    password: options.password === undefined
      ? fromUrl?.password
      : validatePassword(options.password, 'password'),
    tlsOptions: resolveTlsOptions(
      options.tls,
      fromUrl === undefined ? undefined : fromUrl.tls === true
    ),
  };
}

export function parseCacheUrl(url: string): ParsedCacheUrl {
  if (typeof url !== 'string') {
    throw new Error(`invalid CACHE_URL (want yasd://host:port): ${String(url)}`);
  }
  const trimmed = url.trim();
  // yasd://[[user]:password@]host[:port][?poolSize=&password=] — yasds:// enables TLS.
  const match = /^(yasds?):\/\/(?:([^@/?#]*)@)?(\[[^\]]+\]|[^/:?#]+)(?::(\d+))?(?:\?(.*))?$/.exec(trimmed);
  if (!match) throw new Error(`invalid CACHE_URL (want yasd://host:port): ${url}`);
  const tls = match[1] === 'yasds';
  const userinfo = match[2];
  const rawHost = match[3] as string;
  const decodedHost = decodeUrlPart(rawHost, url);
  const host = decodedHost.startsWith('[') && decodedHost.endsWith(']')
    ? decodedHost.slice(1, -1)
    : decodedHost;
  validateHost(host, 'invalid CACHE_URL host');
  const port = match[4] === undefined ? DEFAULT_PORT : parsePort(match[4], 'invalid CACHE_URL port');
  let poolSize: number | undefined;
  let password: string | undefined;
  if (userinfo !== undefined && userinfo.length > 0) {
    const colon = userinfo.indexOf(':');
    decodeUrlPart(colon === -1 ? userinfo : userinfo.slice(0, colon), url);
    password = validatePassword(
      decodeUrlPart(colon === -1 ? userinfo : userinfo.slice(colon + 1), url),
      'CACHE_URL password'
    );
  }
  if (match[5] !== undefined) {
    decodeUrlPart(match[5], url);
    const seen = new Set<string>();
    for (const [key, value] of new URLSearchParams(match[5])) {
      if ((key === 'poolSize' || key === 'password') && seen.has(key)) {
        throw new Error(`invalid CACHE_URL: duplicate ${key}`);
      }
      seen.add(key);
      if (key === 'poolSize') {
        poolSize = parseStrictInteger(value, 'invalid CACHE_URL poolSize', 1, MAX_CLIENT_POOL_SIZE);
      }
      if (key === 'password') password = validatePassword(value, 'CACHE_URL password');
    }
  }
  const out: ParsedCacheUrl = { host, port };
  if (poolSize !== undefined) out.poolSize = poolSize;
  if (password !== undefined) out.password = password;
  if (tls) out.tls = true;
  return out;
}

type Pending = { resolve: (v: RespReply) => void; reject: (e: Error) => void; timer?: ReturnType<typeof setTimeout> };

type SubscriptionCommand = 'subscribe' | 'unsubscribe';

interface SubAckWaiter {
  command: SubscriptionCommand;
  resolve: () => void;
  reject: (e: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

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
  private subDialSocket?: net.Socket;
  private subCommandTail: Promise<void> = Promise.resolve();

  constructor(options: YasdClientOptions = {}) {
    const rawOptions = options as unknown;
    if (rawOptions === null || typeof rawOptions !== 'object' || Array.isArray(rawOptions)) {
      throw new Error('client options must be an object');
    }
    const resolved = resolveClientOptions(rawOptions as YasdClientOptions);
    this.host = resolved.host;
    this.port = resolved.port;
    this.poolSize = resolved.poolSize;
    this.requestTimeoutMs = resolved.requestTimeoutMs;
    this.password = resolved.password;
    this.tlsOptions = resolved.tlsOptions;
  }

  /** Build from `CACHE_URL` (falls back to localhost default when unset). */
  static fromEnv(env: NodeJS.ProcessEnv = process.env): YasdClient {
    return new YasdClient(env.CACHE_URL === undefined ? {} : { url: env.CACHE_URL });
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
    this.connecting = this.replenishPool();
    try {
      await this.connecting;
    } finally {
      this.connecting = undefined;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    this.closeSubSocket(new Error('client is closed'));
    this.subHandlers.clear();
    this.subDecoder.reset();
    const connections = this.pool.splice(0);
    this.roundRobin = 0;
    for (const conn of connections) {
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
  }

  // ---- health ----

  async ping(message?: string): Promise<string> {
    const reply = await this.exec(message === undefined ? ['PING'] : ['PING', message]);
    if (reply.kind === 'simple') return reply.value;
    if (reply.kind === 'bulk' && reply.value !== null) return reply.value;
    throw new Error(`unexpected PING reply: ${JSON.stringify(reply)}`);
  }

  /** HTTP(S) `/healthz` against the server port. Throws when unhealthy. */
  async healthcheck(timeoutMs = 3000): Promise<ServerInfo> {
    const timeout = validateTimeout(timeoutMs, 'healthcheck timeoutMs');
    return new Promise<ServerInfo>((resolve, reject) => {
      const requestOptions: http.RequestOptions = {
        host: this.host,
        port: this.port,
        path: '/healthz',
        timeout,
      };
      const onResponse = (res: http.IncomingMessage): void => {
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
      };
      const req = this.tlsOptions === undefined
        ? http.get(requestOptions, onResponse)
        : https.get({ ...requestOptions, ...this.tlsOptions }, onResponse);
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
    const ttl = ttlMs === undefined ? undefined : validateNonNegativeNumber(ttlMs, 'ttlMs');
    const args = ttl === undefined ? ['SET', key, json] : ['SET', key, json, 'PX', String(ttl)];
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
    const ttl = validateNonNegativeNumber(ttlMs, 'ttlMs');
    return (await this.expectInt(['EXPIRE', key, String(ttl)])) === 1;
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
    const ttl = ttlMs === undefined ? undefined : validateNonNegativeNumber(ttlMs, 'ttlMs');
    const args =
      ttl === undefined
        ? ['CAS', key, expectedArg, valueJson]
        : ['CAS', key, expectedArg, valueJson, 'PX', String(ttl)];
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
    validateNonNegativeSafeInteger(maxRetries, 'maxRetries');
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
    return this.enqueueSubCommand(async () => {
      await this.ensureSubConn();
      let set = this.subHandlers.get(channel);
      if (!set) {
        set = new Set();
        this.subHandlers.set(channel, set);
      }
      const first = set.size === 0;
      set.add(handler);
      if (first) {
        try {
          await this.subRoundTrip('subscribe', channel);
        } catch (err) {
          set.delete(handler);
          if (set.size === 0) this.subHandlers.delete(channel);
          throw err;
        }
      }
      let unsubscribed = false;
      return async (): Promise<void> => {
        if (unsubscribed) return;
        unsubscribed = true;
        await this.enqueueSubCommand(async () => {
          const current = this.subHandlers.get(channel);
          current?.delete(handler);
          if (!current || current.size > 0) return;
          this.subHandlers.delete(channel);
          if (this.subSocket) {
            await this.subRoundTrip('unsubscribe', channel).catch(() => undefined);
          }
          if (this.subHandlers.size === 0) this.closeSubSocket();
        });
      };
    });
  }

  /** Reconnect the subscriber socket and restore all registered channels. */
  async reconnectSubscriptions(): Promise<void> {
    if (this.closed) throw new Error('client is closed');
    await this.enqueueSubCommand(async () => {
      this.closeSubSocket(new Error('subscriber reconnect requested'));
      if (this.subHandlers.size > 0) await this.ensureSubConn();
    });
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
    const existing = this.nextLiveConn();
    if (existing) {
      if (this.pool.length < this.poolSize) {
        void this.connect().catch(() => undefined);
      }
      return existing;
    }
    await this.connect();
    const conn = this.nextLiveConn();
    if (conn) return conn;
    throw new Error('no connections available');
  }

  private nextLiveConn(): PooledConn | undefined {
    this.pruneDeadConnections();
    if (this.pool.length === 0) return undefined;
    for (let i = 0; i < this.pool.length; i++) {
      this.roundRobin = (this.roundRobin + 1) % this.pool.length;
      const conn = this.pool[this.roundRobin] as PooledConn;
      if (!conn.dead && !conn.socket.destroyed) return conn;
    }
    return undefined;
  }

  private async replenishPool(): Promise<void> {
    let lastError: Error | undefined;
    for (let attempt = 0; attempt < RECONNECT_MAX_ATTEMPTS; attempt++) {
      if (this.closed) throw new Error('client is closed');
      this.pruneDeadConnections();
      const needed = this.poolSize - this.pool.length;
      if (needed <= 0) return;
      const created: PooledConn[] = [];
      try {
        for (let i = 0; i < needed; i++) {
          const conn = await this.dialCommand();
          if (this.closed) {
            conn.dead = true;
            conn.socket.destroy();
            throw new Error('client is closed');
          }
          created.push(conn);
        }
      } catch (err) {
        lastError = err as Error;
      }
      if (this.closed) {
        for (const conn of created) {
          conn.dead = true;
          conn.socket.destroy();
        }
        throw new Error('client is closed');
      }
      this.pool.push(...created.filter(conn => !conn.dead && !conn.socket.destroyed));
      this.pruneDeadConnections();
      if (this.pool.length >= this.poolSize) return;
      if (attempt + 1 < RECONNECT_MAX_ATTEMPTS) {
        const delayMs = Math.min(RECONNECT_MAX_DELAY_MS, RECONNECT_BASE_DELAY_MS * 2 ** attempt);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
    throw lastError ?? new Error('could not establish client connections');
  }

  private removeConn(conn: PooledConn): void {
    const index = this.pool.indexOf(conn);
    if (index === -1) return;
    this.pool.splice(index, 1);
    if (this.pool.length === 0) {
      this.roundRobin = 0;
    } else if (this.roundRobin >= this.pool.length) {
      this.roundRobin = this.pool.length - 1;
    }
  }

  private pruneDeadConnections(): void {
    for (const conn of Array.from(this.pool)) {
      if (conn.dead || conn.socket.destroyed) {
        this.failConn(conn, new Error('connection is no longer usable'));
      }
    }
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
        socket.destroy();
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
      if (conn.dead || conn.socket.destroyed) {
        reject(new Error('connection is closed'));
        return;
      }
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
    if (conn.dead) {
      this.removeConn(conn);
      return;
    }
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
    this.removeConn(conn);
  }

  // -- subscriber connection --

  private subAckWaiters = new Map<string, SubAckWaiter[]>();

  private async ensureSubConn(): Promise<void> {
    if (this.closed) throw new Error('client is closed');
    if (this.subSocket && !this.subSocket.destroyed) return;
    if (this.subSocket?.destroyed) {
      this.closeSubSocket(new Error('subscriber connection is closed'));
    }
    if (this.subConnecting) {
      await this.subConnecting;
      return;
    }
    this.subConnecting = this.openSubConnection();
    try {
      await this.subConnecting;
    } finally {
      this.subConnecting = undefined;
    }
  }

  private async openSubConnection(): Promise<void> {
    const channels = Array.from(this.subHandlers.keys());
    const socket = await this.dialRaw();
    this.subDialSocket = socket;
    try {
      if (this.password !== undefined) await this.authenticateSubscriber(socket);
      if (this.closed) throw new Error('client is closed');
      this.subSocket = socket;
      this.subDecoder.reset();
      socket.on('data', chunk => this.onSubData(Buffer.from(chunk), socket));
      socket.on('error', () => this.onSubLost(socket));
      socket.on('close', () => this.onSubLost(socket));
      for (const channel of channels) {
        await this.subRoundTrip('subscribe', channel);
      }
    } catch (err) {
      if (this.subSocket === socket) {
        this.closeSubSocket(err as Error);
      } else {
        socket.destroy();
      }
      throw err;
    } finally {
      if (this.subDialSocket === socket) this.subDialSocket = undefined;
    }
  }

  private authenticateSubscriber(socket: net.Socket): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const decoder = new RespDecoder();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cleanup = (): void => {
        if (timer) clearTimeout(timer);
        socket.off('data', onData);
        socket.off('error', onError);
        socket.off('close', onClose);
      };
      const fail = (err: Error): void => {
        cleanup();
        reject(err);
      };
      const onData = (chunk: Buffer): void => {
        let replies: RespReply[];
        try {
          replies = decoder.push(chunk);
        } catch (err) {
          fail(err as Error);
          return;
        }
        if (replies.length === 0) return;
        const first = replies[0] as RespReply;
        if (first.kind === 'simple' && first.value === 'OK') {
          cleanup();
          resolve();
        } else {
          fail(new Error('authentication failed'));
        }
      };
      const onError = (): void => fail(new Error('subscriber connection error'));
      const onClose = (): void => fail(new Error('subscriber connection closed'));
      socket.on('data', onData);
      socket.once('error', onError);
      socket.once('close', onClose);
      if (this.requestTimeoutMs > 0) {
        timer = setTimeout(() => fail(new Error('subscriber authentication timed out')), this.requestTimeoutMs);
        const t = timer as unknown as { unref?: () => void };
        if (typeof t.unref === 'function') t.unref();
      }
      try {
        socket.write(encodeCommand(['AUTH', this.password as string]));
      } catch (err) {
        fail(err as Error);
      }
    });
  }

  private onSubLost(socket: net.Socket): void {
    if (this.subSocket !== socket) return;
    this.closeSubSocket(new Error('subscriber connection lost'));
  }

  private closeSubSocket(err = new Error('subscriber connection closed')): void {
    const socket = this.subSocket;
    const dialingSocket = this.subDialSocket;
    this.subSocket = undefined;
    this.subDialSocket = undefined;
    this.subDecoder.reset();
    for (const waiters of this.subAckWaiters.values()) {
      for (const waiter of waiters) {
        if (waiter.timer) clearTimeout(waiter.timer);
        waiter.reject(err);
      }
    }
    this.subAckWaiters.clear();
    if (socket) {
      try {
        socket.destroy();
      } catch {
        // ignore
      }
    }
    if (dialingSocket && dialingSocket !== socket) {
      try {
        dialingSocket.destroy();
      } catch {
        // ignore
      }
    }
  }

  private enqueueSubCommand<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.subCommandTail.then(operation, operation);
    this.subCommandTail = result.then(() => undefined, () => undefined);
    return result;
  }

  private onSubData(chunk: Buffer, socket: net.Socket): void {
    if (this.subSocket !== socket) return;
    let replies: RespReply[];
    try {
      replies = this.subDecoder.push(chunk);
    } catch {
      this.closeSubSocket(new Error('subscriber protocol error'));
      return;
    }
    for (const reply of replies) {
      if (reply.kind === 'error') {
        this.rejectNextSubAck(new Error(reply.message.replace(/^ERR\s*/, '')));
        continue;
      }
      if (reply.kind !== 'array') continue;
      const parts = reply.items.map(item => (item && item.kind === 'bulk' ? item.value : null));
      const [kind, channel, payload] = parts;
      if ((kind === 'subscribe' || kind === 'unsubscribe') && typeof channel === 'string') {
        const waiters = this.subAckWaiters.get(channel);
        const index = waiters?.findIndex(waiter => waiter.command === kind) ?? -1;
        if (waiters && index >= 0) {
          const waiter = waiters.splice(index, 1)[0] as SubAckWaiter;
          if (waiter.timer) clearTimeout(waiter.timer);
          if (waiters.length === 0) this.subAckWaiters.delete(channel);
          waiter.resolve();
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

  private rejectNextSubAck(err: Error): void {
    for (const [channel, waiters] of this.subAckWaiters) {
      const waiter = waiters.shift();
      if (!waiter) continue;
      if (waiter.timer) clearTimeout(waiter.timer);
      if (waiters.length === 0) this.subAckWaiters.delete(channel);
      waiter.reject(err);
      this.closeSubSocket(err);
      return;
    }
    this.closeSubSocket(err);
  }

  private subRoundTrip(command: SubscriptionCommand, channel: string): Promise<void> {
    const socket = this.subSocket;
    if (!socket || socket.destroyed) return Promise.reject(new Error('subscriber connection is closed'));
    return new Promise<void>((resolve, reject) => {
      let waiters = this.subAckWaiters.get(channel);
      if (!waiters) {
        waiters = [];
        this.subAckWaiters.set(channel, waiters);
      }
      const waiter: SubAckWaiter = { command, resolve, reject };
      waiters.push(waiter);
      const removeWaiter = (): boolean => {
        const current = this.subAckWaiters.get(channel);
        if (!current) return false;
        const index = current.indexOf(waiter);
        if (index === -1) return false;
        current.splice(index, 1);
        if (current.length === 0) this.subAckWaiters.delete(channel);
        return true;
      };
      try {
        socket.write(encodeCommand([command === 'subscribe' ? 'SUBSCRIBE' : 'UNSUBSCRIBE', channel]));
      } catch (err) {
        removeWaiter();
        reject(err as Error);
        this.closeSubSocket(err as Error);
        return;
      }
      if (this.requestTimeoutMs > 0) {
        waiter.timer = setTimeout(() => {
          if (!removeWaiter()) return;
          const timeout = new Error(`${command} timed out: ${channel}`);
          reject(timeout);
          this.closeSubSocket(timeout);
        }, this.requestTimeoutMs);
        const t = waiter.timer as unknown as { unref?: () => void };
        if (typeof t.unref === 'function') t.unref();
      }
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
    this.host = validateHost(options.host, 'host');
    this.port = validatePort(options.port, 'port');
    this.password = options.password;
    this.tlsOptions = resolveTlsOptions(options.tlsOptions, undefined, 'tlsOptions');
    this.requestTimeoutMs = options.requestTimeoutMs === undefined
      ? 5000
      : validateTimeout(options.requestTimeoutMs, 'requestTimeoutMs');
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
    const ttl = ttlMs === undefined ? undefined : validateNonNegativeNumber(ttlMs, 'ttlMs');
    await this.queue(
      ttl === undefined ? ['SET', key, json] : ['SET', key, json, 'PX', String(ttl)]
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
    const ttl = validateNonNegativeNumber(ttlMs, 'ttlMs');
    await this.queue(['EXPIRE', key, String(ttl)]);
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
    const ttl = ttlMs === undefined ? undefined : validateNonNegativeNumber(ttlMs, 'ttlMs');
    await this.queue(
      ttl === undefined
        ? ['CAS', key, expectedArg, valueJson]
        : ['CAS', key, expectedArg, valueJson, 'PX', String(ttl)]
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
