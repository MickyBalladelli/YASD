// Shared metrics primitives for YASD.
// `SlowLog` backs both the embedded SQL slow-query log (`Executor`) and the
// server slow-command log (`YasdServer`), so "slow" means the same thing
// everywhere: entries slower than `thresholdMs`, newest-first, capped.

import {
  validateNonNegativeNumber,
  validatePositiveSafeInteger,
} from "./validation";

/** One slow operation: SQL text or `CMD argc`, duration, wall-clock time. */
export interface SlowEntry {
  /** SQL text (executor) or upper-case command name (server). */
  name: string;
  /** Measured duration in ms (sub-ms precision). */
  durationMs: number;
  /** Wall-clock time of completion (epoch ms). */
  at: number;
  /** Command arity excluding the command itself (server only). */
  argc?: number;
}

export const SLOW_LOG_CAP = 100;

export function checkSlowThreshold(ms: number, what: string): number {
  return validateNonNegativeNumber(ms, what);
}

/**
 * Newest-first ring of slow operations, capped at `cap` (default 100).
 * A threshold of 0 disables recording. Fractional thresholds are allowed —
 * durations are measured with sub-ms precision, so e.g. `1e-6` logs
 * everything (handy for tests).
 */
export class SlowLog {
  private entries: SlowEntry[] = [];
  private thresholdMs: number;
  private readonly cap: number;

  constructor(thresholdMs = 0, cap: number = SLOW_LOG_CAP) {
    this.thresholdMs = checkSlowThreshold(thresholdMs, "slow threshold");
    this.cap = validatePositiveSafeInteger(cap, "slow log cap");
  }

  get threshold(): number {
    return this.thresholdMs;
  }

  setThreshold(ms: number): void {
    this.thresholdMs = checkSlowThreshold(ms, "slow threshold");
  }

  /**
   * Record `name` when enabled and `durationMs >= threshold`. Returns true
   * when the entry was logged. `at` defaults to now (epoch ms).
   */
  record(
    name: string,
    durationMs: number,
    argc?: number,
    at?: number,
  ): boolean {
    if (!(this.thresholdMs > 0)) return false;
    if (!(durationMs >= this.thresholdMs)) return false;
    this.entries.unshift(
      argc === undefined
        ? { name, durationMs, at: at ?? Date.now() }
        : { name, durationMs, at: at ?? Date.now(), argc },
    );
    if (this.entries.length > this.cap) this.entries.length = this.cap;
    return true;
  }

  /** Newest-first copy of the ring. */
  list(): SlowEntry[] {
    return [...this.entries];
  }

  clear(): void {
    this.entries = [];
  }

  get size(): number {
    return this.entries.length;
  }
}
