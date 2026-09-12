import { Value } from './types';
import { DatabaseError } from './errors';

/** Canonical structural PK identity: type-preserving, sorted object keys, ordered arrays. */
export function valueIdentity(value: Value): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(valueIdentity).join(',')}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${valueIdentity(value[key])}`).join(',')}}`;
}

/** Keep the smallest k values in a max heap; O(n log k), O(k) retained rows. */
export class TopK<T> {
  private heap: T[] = [];
  constructor(private readonly k: number, private readonly compare: (a: T, b: T) => number) {}
  add(value: T): void {
    if (this.k === 0) return;
    const h = this.heap;
    if (h.length < this.k) {
      h.push(value); let i = h.length - 1;
      while (i > 0) {
        const parent = (i - 1) >> 1;
        if (this.compare(h[parent], h[i]) >= 0) break;
        [h[parent], h[i]] = [h[i], h[parent]]; i = parent;
      }
    } else if (this.compare(value, h[0]) < 0) {
      h[0] = value; let i = 0;
      for (;;) {
        let child = i * 2 + 1; if (child >= h.length) break;
        if (child + 1 < h.length && this.compare(h[child + 1], h[child]) > 0) child++;
        if (this.compare(h[i], h[child]) >= 0) break;
        [h[i], h[child]] = [h[child], h[i]]; i = child;
      }
    }
  }
  sorted(): T[] { return this.heap.sort(this.compare); }
}

/** Greedy wildcard matcher with an explicit work cap; no regex backtracking. */
export function matchLike(value: string, pattern: string): boolean {
  const text = value.toLowerCase(); const pat = pattern.toLowerCase();
  let i = 0; let j = 0; let star = -1; let retry = 0; let steps = 0;
  while (i < text.length) {
    if (++steps > 1_000_000) throw new DatabaseError('LIKE work limit exceeded', 'LIMIT_EXCEEDED');
    if (j < pat.length && (pat[j] === '_' || pat[j] === text[i])) { i++; j++; }
    else if (pat[j] === '%') { star = j++; retry = i; }
    else if (star >= 0) { j = star + 1; i = ++retry; }
    else return false;
  }
  while (pat[j] === '%') j++;
  return j === pat.length;
}
