/** Ordered map with nested, touched-state undo journals. Values must be immutable. */
interface Link<K, V> { key: K; value: V; prev?: Link<K, V>; next?: Link<K, V> }
export class OrderedMap<K, V> implements Iterable<[K, V]> {
  private nodes = new Map<K, Link<K, V>>();
  private first?: Link<K, V>;
  private last?: Link<K, V>;
  private undo: Array<() => void> = [];
  private frames: number[] = [];
  constructor(private readonly changed?: (key: K, value: V | undefined) => void) {}
  get size(): number { return this.nodes.size; }
  get journalSize(): number { return this.undo.length; }
  get(key: K): V | undefined { return this.nodes.get(key)?.value; }
  has(key: K): boolean { return this.nodes.has(key); }
  begin(): void { this.frames.push(this.undo.length); }
  commit(): void {
    if (this.frames.pop() === undefined) throw new Error('No ordered-map transaction');
    if (!this.frames.length) this.undo = [];
  }
  rollback(): void {
    const start = this.frames.pop();
    if (start === undefined) throw new Error('No ordered-map transaction');
    for (let i = this.undo.length - 1; i >= start; i--) this.undo[i]();
    this.undo.length = start;
  }
  private record(fn: () => void): void { if (this.frames.length) this.undo.push(fn); }
  private unlink(node: Link<K, V>): void {
    if (node.prev) node.prev.next = node.next; else this.first = node.next;
    if (node.next) node.next.prev = node.prev; else this.last = node.prev;
    this.nodes.delete(node.key);
  }
  private link(node: Link<K, V>, prev?: Link<K, V>, next?: Link<K, V>): void {
    node.prev = prev; node.next = next;
    if (prev) prev.next = node; else this.first = node;
    if (next) next.prev = node; else this.last = node;
    this.nodes.set(node.key, node);
  }
  set(key: K, value: V): this {
    const node = this.nodes.get(key);
    if (node) {
      const old = node.value; this.record(() => { node.value = old; }); node.value = value;
    } else {
      const added = { key, value };
      this.link(added, this.last); this.record(() => this.unlink(added));
    }
    this.changed?.(key, value); return this;
  }
  delete(key: K): boolean {
    const node = this.nodes.get(key); if (!node) return false;
    const { prev, next } = node;
    this.record(() => this.link(node, prev, next)); this.unlink(node);
    this.changed?.(key, undefined); return true;
  }
  clear(): void { for (const key of this.keys()) this.delete(key); }
  *entries(): IterableIterator<[K, V]> {
    let node = this.first;
    while (node) { const current = node; node = node.next; yield [current.key, current.value]; }
  }
  *keys(): IterableIterator<K> { for (const [key] of this.entries()) yield key; }
  *values(): IterableIterator<V> { for (const [, value] of this.entries()) yield value; }
  [Symbol.iterator](): IterableIterator<[K, V]> { return this.entries(); }
}
