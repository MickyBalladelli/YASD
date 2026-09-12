// Cache-aside example, not replication. The durable adapter returns plain JSON
// posts (not a SQL driver's result envelope), or null/undefined for a missing post.
// All writers must use the invalidation path below. Cross-host clocks must be
// synchronized for the source-age bound; future timestamps are rejected.
// A durable commit and a cache invalidation are not a distributed transaction:
// invalidation failure can leave stale data until its absolute age bound expires.
const { INVALIDATE_CHANNEL, KVCache } = require("../dist/index.js");

const CACHE_KEY_VERSION = "v3";
const CACHE_PREFIX = `app:${CACHE_KEY_VERSION}:post:`;
const CACHE_TTL_MS = 30_000;
const LOCAL_FRESH_MS = 5_000;
const LOCAL_STALE_MAX_MS = 60_000;
const cacheKey = (id) => `${CACHE_PREFIX}${encodeURIComponent(String(id))}`;
const isRecord = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const copy = (value) =>
  value === undefined ? undefined : JSON.parse(JSON.stringify(value));

function decodeEntry(value, now = Date.now()) {
  if (
    !isRecord(value) ||
    value.cacheKeyVersion !== CACHE_KEY_VERSION ||
    value.deleted ||
    !Number.isFinite(value.cachedAt) ||
    value.cachedAt > now ||
    now - value.cachedAt > LOCAL_STALE_MAX_MS ||
    !Object.hasOwn(value, "value")
  )
    return undefined;
  return copy(value);
}

class ProductionPostCache {
  constructor({
    client,
    durableStore,
    logger = console,
    maxLocalEntries = 5000,
    maxLocalBytes = 8 * 1024 * 1024,
    maxRefreshes = 128,
  }) {
    if (!client) throw new Error("client is required");
    for (const name of ["getPost", "savePost", "deletePost"]) {
      if (!durableStore || typeof durableStore[name] !== "function")
        throw new Error(`durableStore.${name} is required`);
    }
    if (!Number.isSafeInteger(maxRefreshes) || maxRefreshes < 1)
      throw new Error("maxRefreshes must be positive");
    this.client = client;
    this.durableStore = durableStore;
    this.logger = logger;
    this.local = new KVCache({
      maxEntries: maxLocalEntries,
      maxBytes: maxLocalBytes,
      sweepIntervalMs: 0,
    });
    this.refreshes = new Map();
    this.activeRefreshes = 0;
    this.maxRefreshes = maxRefreshes;
    // One conservative process epoch bounds metadata and fences every in-flight fill.
    this.epoch = 0;
  }

  invalidateAll() {
    this.epoch++;
    this.local.clear();
  }

  async start() {
    await this.client.connect();
    if (this.client.onSubscriptionState) {
      this.stopStatus = this.client.onSubscriptionState(() =>
        this.invalidateAll(),
      );
    }
    this.stopInvalidations = await this.client.subscribe(
      INVALIDATE_CHANNEL,
      (_channel, message) => this.handleInvalidation(message),
    );
  }

  async stop() {
    this.invalidateAll();
    this.stopStatus?.();
    if (this.stopInvalidations) await this.stopInvalidations();
    this.stopInvalidations = undefined;
    this.local.close();
  }

  async reconnect() {
    this.invalidateAll();
    await this.client.reconnectSubscriptions();
  }

  handleInvalidation(message) {
    let event;
    try {
      event = JSON.parse(message);
    } catch {
      this.logger.warn("ignored malformed invalidation");
      return;
    }
    if (!isRecord(event)) return;
    if (
      event.event === "load" ||
      (typeof event.key === "string" && event.key.startsWith(CACHE_PREFIX)) ||
      (event.event === "clear" &&
        typeof event.prefix === "string" &&
        (CACHE_PREFIX.startsWith(event.prefix) ||
          event.prefix.startsWith(CACHE_PREFIX)))
    )
      this.invalidateAll();
  }

  remember(key, entry) {
    const remaining = LOCAL_STALE_MAX_MS - (Date.now() - entry.cachedAt);
    if (remaining <= 0) return;
    try {
      this.local.set(key, entry, remaining);
    } catch {
      /* Oversized entries still return to the caller without entering L1. */
    }
  }

  async getPost(id, { allowStale = true } = {}) {
    const key = cacheKey(id);
    const epoch = this.epoch;
    let stale = decodeEntry(this.local.get(key));
    if (stale && Date.now() - stale.cachedAt <= LOCAL_FRESH_MS)
      return copy(stale.value);
    try {
      const remote = decodeEntry(await this.client.get(key));
      if (remote && epoch === this.epoch) {
        stale = remote;
        this.remember(key, remote);
        if (Date.now() - remote.cachedAt <= LOCAL_FRESH_MS)
          return copy(remote.value);
        if (allowStale) {
          void this.refreshPost(id).catch(() =>
            this.logger.warn("background cache refresh failed"),
          );
          return copy(remote.value);
        }
      }
    } catch {
      /* Read the source of truth, not an invented fallback result. */
    }
    try {
      return await this.refreshPost(id);
    } catch (error) {
      if (allowStale && epoch === this.epoch && decodeEntry(stale))
        return copy(stale.value);
      throw error;
    }
  }

  async refreshPost(id) {
    const key = cacheKey(id);
    const running = this.refreshes.get(key);
    if (running && running.epoch === this.epoch)
      return copy(await running.promise);
    if (this.activeRefreshes >= this.maxRefreshes)
      throw new Error("cache refresh concurrency limit reached");
    this.activeRefreshes++;
    const operation = { epoch: this.epoch };
    operation.promise = this.refreshPostOnce(id, operation.epoch).finally(
      () => {
        this.activeRefreshes--;
        if (this.refreshes.get(key) === operation) this.refreshes.delete(key);
      },
    );
    this.refreshes.set(key, operation);
    return copy(await operation.promise);
  }

  async refreshPostOnce(id, epoch) {
    const key = cacheKey(id);
    const tx = this.client.multi();
    let watching = false;
    try {
      try {
        await tx.watch(key);
        watching = true;
      } catch {
        /* Durable read remains available. */
      }
      // Count source-read duration as age too: a slow read cannot extend freshness.
      const cachedAt = Date.now();
      const post = await this.durableStore.getPost(id);
      const entry =
        post == null
          ? { cacheKeyVersion: CACHE_KEY_VERSION, cachedAt, deleted: true }
          : { cacheKeyVersion: CACHE_KEY_VERSION, cachedAt, value: copy(post) };
      const remaining = CACHE_TTL_MS - (Date.now() - cachedAt);
      if (watching && epoch === this.epoch && remaining > 0) {
        try {
          await tx.set(key, entry, remaining);
          const committed = await tx.exec();
          if (committed !== null && epoch === this.epoch && !entry.deleted)
            this.remember(key, entry);
        } catch {
          this.logger.warn("cache fill failed; durable read succeeded");
        }
      }
      return post == null ? undefined : copy(post);
    } finally {
      await tx.close();
    }
  }

  async invalidatePost(id) {
    this.invalidateAll();
    // A tombstone SET changes WATCH even when the cache key was already absent.
    // DEL on a missing key would leave a race in a concurrent initial fill.
    try {
      await this.client.set(
        cacheKey(id),
        {
          cacheKeyVersion: CACHE_KEY_VERSION,
          cachedAt: Date.now(),
          deleted: true,
        },
        CACHE_TTL_MS,
      );
    } catch {
      this.logger.error(
        "durable commit succeeded but cache invalidation failed",
      );
    }
  }

  async savePost(id, post) {
    this.invalidateAll();
    try {
      return await this.durableStore.savePost(id, post);
    } finally {
      await this.invalidatePost(id);
    }
  }

  async deletePost(id) {
    this.invalidateAll();
    try {
      return await this.durableStore.deletePost(id);
    } finally {
      await this.invalidatePost(id);
    }
  }
}

module.exports = {
  ProductionPostCache,
  CACHE_KEY_VERSION,
  CACHE_PREFIX,
  CACHE_TTL_MS,
  LOCAL_FRESH_MS,
  LOCAL_STALE_MAX_MS,
  decodeEntry,
};
