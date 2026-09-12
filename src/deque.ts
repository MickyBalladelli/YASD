/** FIFO with O(1) removal and eager reference release, without Array.shift copying. */
export class Deque<T> implements Iterable<T> {
  private items = new Map<number, T>();
  private head = 0;
  private tail = 0;
  get length(): number {
    return this.items.size;
  }
  set length(value: number) {
    if (value !== 0) throw new Error("Deque only supports length = 0");
    this.clear();
  }
  push(value: T): number {
    this.items.set(this.tail++, value);
    return this.length;
  }
  shift(): T | undefined {
    if (!this.length) return undefined;
    const value = this.items.get(this.head);
    this.items.delete(this.head++);
    if (!this.length) this.clear();
    return value;
  }
  pop(): T | undefined {
    if (!this.length) return undefined;
    const value = this.items.get(--this.tail);
    this.items.delete(this.tail);
    if (!this.length) this.clear();
    return value;
  }
  indexOf(value: T): number {
    let index = 0;
    for (const item of this.items.values()) {
      if (item === value) return index;
      index++;
    }
    return -1;
  }
  splice(start: number): T[] {
    if (start !== 0) throw new Error("Deque only supports draining splice(0)");
    const result = [...this.items.values()];
    this.clear();
    return result;
  }
  clear(): void {
    this.items.clear();
    this.head = 0;
    this.tail = 0;
  }
  [Symbol.iterator](): IterableIterator<T> {
    return this.items.values();
  }
}
