// Production integration pattern.
//
// YASD is a cache, not the durable source of truth. Inject a repository backed
// by Postgres, MySQL, MongoDB, or another durable application database.
// The repository shape is:
//   { getPost(id), savePost(id, post), deletePost(id) }
//
// This example uses a versioned cache key, a short fresh window, bounded stale
// reads, and the YASD invalidation channel for a process-local L1 cache.

const { INVALIDATE_CHANNEL } = require('../dist/index.js')

const CACHE_KEY_VERSION = 'v2'
const CACHE_PREFIX = `app:${CACHE_KEY_VERSION}:post:`
const CACHE_TTL_MS = 30_000
const LOCAL_FRESH_MS = 5_000
const LOCAL_STALE_MAX_MS = 60_000

function cacheKey(id) {
  return `${CACHE_PREFIX}${encodeURIComponent(String(id))}`
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function decodeEntry(value) {
  if (
    !isRecord(value) ||
    value.cacheKeyVersion !== CACHE_KEY_VERSION ||
    typeof value.cachedAt !== 'number' ||
    !Number.isFinite(value.cachedAt) ||
    !Object.prototype.hasOwnProperty.call(value, 'value')
  ) return undefined
  // Age is measured when this process observes the entry. The YASD TTL
  // bounds remote-cache age; this avoids trusting another host's wall clock.
  return { value: value.value, cachedAt: Date.now() }
}

function ageMs(entry, now = Date.now()) {
  return Math.max(0, now - entry.cachedAt)
}

class ProductionPostCache {
  constructor({ client, durableStore, logger = console }) {
    if (!client) throw new Error('client is required')
    if (!durableStore || typeof durableStore.getPost !== 'function') {
      throw new Error('durableStore.getPost is required')
    }
    if (typeof durableStore.savePost !== 'function') {
      throw new Error('durableStore.savePost is required')
    }
    if (typeof durableStore.deletePost !== 'function') {
      throw new Error('durableStore.deletePost is required')
    }
    this.client = client
    this.durableStore = durableStore
    this.logger = logger
    this.local = new Map()
    this.refreshes = new Map()
    this.stopInvalidations = undefined
  }

  async start() {
    await this.client.connect()
    this.stopInvalidations = await this.client.subscribe(
      INVALIDATE_CHANNEL,
      (_channel, message) => this.handleInvalidation(message)
    )
  }

  async stop() {
    if (this.stopInvalidations) {
      await this.stopInvalidations()
      this.stopInvalidations = undefined
    }
  }

  // Subscription reconnects do not replay the gap. Drop local state first;
  // the next read will refill from YASD or the durable source.
  async reconnect() {
    this.local.clear()
    await this.client.reconnectSubscriptions()
  }

  handleInvalidation(message) {
    let event
    try {
      event = JSON.parse(message)
    } catch {
      this.logger.warn('ignored malformed YASD invalidation')
      return
    }
    if (!isRecord(event)) return

    if (event.event === 'load') {
      this.local.clear()
      return
    }
    if (typeof event.key === 'string' && event.key.startsWith(CACHE_PREFIX)) {
      this.local.delete(event.key)
    }
    if (event.event === 'clear' && typeof event.prefix === 'string') {
      const prefix = event.prefix
      if (
        CACHE_PREFIX === prefix ||
        CACHE_PREFIX.startsWith(`${prefix}:`) ||
        prefix.startsWith(CACHE_PREFIX)
      ) this.local.clear()
    }
  }

  async getPost(id, { allowStale = true } = {}) {
    const key = cacheKey(id)
    const localEntry = this.local.get(key)
    if (localEntry && ageMs(localEntry) <= LOCAL_FRESH_MS) {
      return localEntry.value
    }

    let cached
    try {
      cached = decodeEntry(await this.client.get(key))
    } catch (error) {
      return this.readDurableOrStale(id, localEntry, allowStale, error)
    }

    if (cached) {
      this.local.set(key, cached)
      const age = ageMs(cached)
      if (age <= LOCAL_FRESH_MS) return cached.value
      if (allowStale && age <= LOCAL_STALE_MAX_MS) {
        void this.refreshPost(id).catch(error => {
          this.logger.warn(`background refresh failed for post ${id}: ${error.message}`)
        })
        return cached.value
      }
    }

    return this.readDurableOrStale(id, cached || localEntry, allowStale)
  }

  async readDurableOrStale(id, staleEntry, allowStale, cacheError) {
    try {
      return await this.refreshPost(id)
    } catch (error) {
      if (allowStale && staleEntry && ageMs(staleEntry) <= LOCAL_STALE_MAX_MS) {
        this.logger.warn(
          `serving bounded stale post ${id}: ${(cacheError || error).message}`
        )
        return staleEntry.value
      }
      throw error
    }
  }

  async refreshPost(id) {
    const key = cacheKey(id)
    const running = this.refreshes.get(key)
    if (running) return running

    const refresh = this.refreshPostOnce(id).finally(() => {
      this.refreshes.delete(key)
    })
    this.refreshes.set(key, refresh)
    return refresh
  }

  async refreshPostOnce(id) {
    const key = cacheKey(id)
    const post = await this.durableStore.getPost(id)
    if (post === undefined || post === null) {
      this.local.delete(key)
      await this.client.del(key).catch(error => {
        this.logger.warn(`cache delete failed for missing post ${id}: ${error.message}`)
      })
      return undefined
    }

    const entry = {
      cacheKeyVersion: CACHE_KEY_VERSION,
      cachedAt: Date.now(),
      value: post,
    }
    this.local.set(key, entry)
    await this.client.set(key, entry, CACHE_TTL_MS).catch(error => {
      // The durable read still succeeds. The bounded local entry protects
      // this request; later requests retry YASD and then the durable source.
      this.logger.warn(`cache fill failed for post ${id}: ${error.message}`)
    })
    return post
  }

  async savePost(id, post) {
    // Commit durable data first. Cache invalidation is only a hint.
    const saved = await this.durableStore.savePost(id, post)
    const key = cacheKey(id)
    this.local.delete(key)
    await this.client.del(key).catch(error => {
      this.logger.error(`cache invalidation failed for post ${id}: ${error.message}`)
    })
    return saved
  }

  async deletePost(id) {
    const deleted = await this.durableStore.deletePost(id)
    const key = cacheKey(id)
    this.local.delete(key)
    await this.client.del(key).catch(error => {
      this.logger.error(`cache invalidation failed for deleted post ${id}: ${error.message}`)
    })
    return deleted
  }
}

module.exports = {
  ProductionPostCache,
  CACHE_KEY_VERSION,
  CACHE_PREFIX,
  CACHE_TTL_MS,
  LOCAL_FRESH_MS,
  LOCAL_STALE_MAX_MS,
}
