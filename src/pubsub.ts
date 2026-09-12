// In-process pub/sub hub for YASD.
// Used for cross-replica cache invalidation plus Echo presence/typing fanout.
// Messages are strings (JSON-encode structured payloads at the call site).
// Delivery is synchronous, in subscription order; a throwing listener never
// breaks delivery to the remaining listeners.

/** Well-known channel the server publishes key invalidation events on. */
export const INVALIDATE_CHANNEL = "__yasd__:invalidate";

export type PubSubListener = (channel: string, message: string) => void;

export interface InvalidationEvent {
  event: "set" | "del" | "clear" | "expire" | "persist" | "load";
  key?: string;
  prefix?: string;
}

export function invalidateMessage(ev: InvalidationEvent): string {
  return JSON.stringify(ev);
}

export class PubSubHub {
  private channels = new Map<string, Set<PubSubListener>>();

  /** Subscribe; returns an unsubscribe function. */
  subscribe(channel: string, listener: PubSubListener): () => void {
    if (typeof channel !== "string" || channel.length === 0) {
      throw new Error("subscribe requires a non-empty channel name");
    }
    if (typeof listener !== "function") {
      throw new Error("subscribe requires a listener function");
    }
    let set = this.channels.get(channel);
    if (!set) {
      set = new Set();
      this.channels.set(channel, set);
    }
    set.add(listener);
    return () => this.unsubscribe(channel, listener);
  }

  unsubscribe(channel: string, listener: PubSubListener): boolean {
    const set = this.channels.get(channel);
    if (!set) return false;
    const removed = set.delete(listener);
    if (set.size === 0) this.channels.delete(channel);
    return removed;
  }

  /** Remove all listeners, optionally limited to one channel. */
  unsubscribeAll(channel?: string): number {
    if (channel !== undefined) {
      const set = this.channels.get(channel);
      if (!set) return 0;
      const n = set.size;
      this.channels.delete(channel);
      return n;
    }
    let n = 0;
    for (const set of this.channels.values()) n += set.size;
    this.channels.clear();
    return n;
  }

  /** Publish; returns the number of listeners that received the message. */
  publish(channel: string, message: string): number {
    const set = this.channels.get(channel);
    if (!set || set.size === 0) return 0;
    const msg = String(message);
    let delivered = 0;
    for (const listener of Array.from(set)) {
      try {
        listener(channel, msg);
      } catch {
        // One bad listener must not break the rest.
      }
      delivered++;
    }
    return delivered;
  }

  subscriberCount(channel?: string): number {
    if (channel !== undefined) return this.channels.get(channel)?.size ?? 0;
    let n = 0;
    for (const set of this.channels.values()) n += set.size;
    return n;
  }

  channelNames(): string[] {
    return Array.from(this.channels.keys());
  }
}
