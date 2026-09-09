// Persistence for YASD cache: snapshots + append-only log (AOF).
// A cache stays ephemeral by design — this exists for restart-storm
// protection: SAVE/LOAD a full snapshot, replay the AOF on startup.

import * as fs from 'fs';
import * as path from 'path';
import { KVCache, SnapshotEntry, KVBatchEntry } from './cache';

export interface SnapshotFile {
  version: 1;
  savedAt: number;
  entries: SnapshotEntry[];
}

/** Structural store for snapshots (satisfied by KVCache and YASD). */
export interface SnapshotStore {
  dump(): SnapshotEntry[];
  clear(): void;
  restore(entries: SnapshotEntry[]): number;
}

/** Replayable mutation ops for the append-only log. */
export type AofOp =
  | { op: 'set'; key: string; value: SnapshotEntry['value']; ttlMs?: number }
  | { op: 'mset'; entries: KVBatchEntry[] }
  | { op: 'del'; keys: string[] }
  | { op: 'clear'; prefix: string }
  | { op: 'expire'; key: string; ttlMs: number }
  | { op: 'persist'; key: string }
  | { op: 'incr'; key: string; by: number };

function ensureDir(filePath: string): void {
  const dir = path.dirname(filePath);
  if (dir && dir !== '.' && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/** Per-call counter so concurrent saves never share a tmp path. */
let saveSnapshotCounter = 0;

/** Write a full snapshot. Returns the number of entries saved. */
export async function saveSnapshot(store: SnapshotStore, filePath: string): Promise<number> {
  const entries = store.dump();
  const file: SnapshotFile = { version: 1, savedAt: Date.now(), entries };
  ensureDir(filePath);
  // Unique tmp path per call: concurrent saves (periodic autosave, explicit
  // SAVE, shutdown SAVE) must not share one tmp file — the loser of a
  // write/rename overlap would hit ENOENT on rename.
  const tmp = `${filePath}.tmp.${process.pid}.${saveSnapshotCounter++}`;
  await fs.promises.writeFile(tmp, JSON.stringify(file), 'utf8');
  await fs.promises.rename(tmp, filePath);
  return entries.length;
}

/**
 * Load a snapshot. By default clears first (clean restart restore).
 * Returns the number of entries restored (0 when the file is absent and
 * `missingOk` is set).
 */
export async function loadSnapshot(
  store: SnapshotStore,
  filePath: string,
  options: { clearFirst?: boolean; missingOk?: boolean } = {}
): Promise<number> {
  const { clearFirst = true, missingOk = true } = options;
  let raw: string;
  try {
    raw = await fs.promises.readFile(filePath, 'utf8');
  } catch (err) {
    if (missingOk && (err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
    throw err;
  }
  const file = JSON.parse(raw) as SnapshotFile;
  if (!file || !Array.isArray(file.entries)) {
    throw new Error(`invalid snapshot file: ${filePath}`);
  }
  if (clearFirst) store.clear();
  return store.restore(file.entries);
}

/** Apply one AOF op without producing further log output. */
export function applyAofOp(cache: KVCache, op: AofOp): void {
  switch (op.op) {
    case 'set':
      cache.set(op.key, op.value, op.ttlMs);
      break;
    case 'mset':
      cache.mset(op.entries);
      break;
    case 'del':
      for (const key of op.keys) cache.del(key);
      break;
    case 'clear':
      cache.clearPrefix(op.prefix);
      break;
    case 'expire':
      cache.expire(op.key, op.ttlMs);
      break;
    case 'persist':
      cache.persist(op.key);
      break;
    case 'incr':
      cache.incr(op.key, op.by);
      break;
  }
}

/**
 * Append-only mutation log. Appends are synchronous (`appendFileSync`) so a
 * crash loses nothing acknowledged. Replay skips corrupt trailing lines.
 */
export class AofLog {
  private filePath: string | undefined;

  constructor(filePath?: string) {
    this.filePath = filePath;
    if (filePath) ensureDir(filePath);
  }

  get enabled(): boolean {
    return this.filePath !== undefined;
  }

  get path(): string | undefined {
    return this.filePath;
  }

  append(op: AofOp): void {
    if (!this.filePath) return;
    fs.appendFileSync(this.filePath, JSON.stringify(op) + '\n', 'utf8');
  }

  /** Replay the log onto a cache. Returns the number of ops applied. */
  async replay(cache: KVCache): Promise<number> {
    if (!this.filePath) return 0;
    let raw: string;
    try {
      raw = await fs.promises.readFile(this.filePath, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
      throw err;
    }
    let applied = 0;
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        applyAofOp(cache, JSON.parse(trimmed) as AofOp);
        applied++;
      } catch {
        // Skip corrupt lines (e.g. torn trailing write) and continue.
      }
    }
    return applied;
  }

  /** Truncate the log (used after SAVE rewrites state into a snapshot). */
  async truncate(): Promise<void> {
    if (!this.filePath) return;
    await fs.promises.writeFile(this.filePath, '', 'utf8');
  }
}
