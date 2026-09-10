// Persistence for YASD cache: snapshots + append-only log (AOF).
// A cache stays ephemeral by design — this exists for restart-storm
// protection: SAVE/LOAD a full snapshot, replay the AOF on startup.

import * as fs from 'fs';
import * as path from 'path';
import { KVCache, SnapshotEntry, KVBatchEntry } from './cache';

export interface SnapshotFile {
  version: 1 | 2;
  savedAt: number;
  entries: SnapshotEntry[];
  /** Highest AOF sequence already represented by this snapshot. */
  aofSeq?: number;
}

export interface SnapshotSaveOptions {
  /** Highest AOF sequence already represented by the snapshot. */
  aofSeq?: number;
}

export interface SnapshotLoadMetadata {
  /** Highest AOF sequence already represented by the loaded snapshot. */
  aofSeq?: number;
}

export interface SnapshotLoadOptions {
  clearFirst?: boolean;
  missingOk?: boolean;
  metadata?: SnapshotLoadMetadata;
}

/** Structural store for snapshots (satisfied by KVCache and YASD). */
export interface SnapshotStore {
  dump(): SnapshotEntry[];
  clear(): void;
  restore(entries: SnapshotEntry[]): number;
}

/** One replayable mutation inside an AOF transaction. */
export type AofMutation =
  | { op: 'set'; key: string; value: SnapshotEntry['value']; ttlMs?: number }
  | { op: 'mset'; entries: KVBatchEntry[] }
  | { op: 'del'; keys: string[] }
  | { op: 'clear'; prefix: string }
  | { op: 'expire'; key: string; ttlMs: number }
  | { op: 'persist'; key: string }
  | { op: 'incr'; key: string; by: number };

/** Replayable mutation ops for the append-only log. */
export type AofOp = AofMutation | { op: 'transaction'; ops: AofMutation[] };

function ensureDir(filePath: string): void {
  const dir = path.dirname(filePath);
  if (dir && dir !== '.' && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/** Per-call counter so concurrent saves never share a tmp path. */
let saveSnapshotCounter = 0;

function checkAofSeq(value: number, what: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${what} must be a safe integer >= 0`);
  }
  return value;
}

async function writeDurableFile(filePath: string, contents: string): Promise<void> {
  const handle = await fs.promises.open(filePath, 'w');
  try {
    await handle.writeFile(contents, 'utf8');
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function syncDirectory(filePath: string): Promise<void> {
  const handle = await fs.promises.open(path.dirname(filePath), 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/** Write a full snapshot. Returns the number of entries saved. */
export async function saveSnapshot(
  store: SnapshotStore,
  filePath: string,
  options: SnapshotSaveOptions = {}
): Promise<number> {
  const entries = store.dump();
  const aofSeq = options.aofSeq === undefined ? undefined : checkAofSeq(options.aofSeq, 'aofSeq');
  const file: SnapshotFile = {
    version: 2,
    savedAt: Date.now(),
    entries,
    ...(aofSeq === undefined ? {} : { aofSeq }),
  };
  ensureDir(filePath);
  // Unique tmp path per call: concurrent saves (periodic autosave, explicit
  // SAVE, shutdown SAVE) must not share one tmp file — the loser of a
  // write/rename overlap would hit ENOENT on rename.
  const tmp = `${filePath}.tmp.${process.pid}.${saveSnapshotCounter++}`;
  await writeDurableFile(tmp, JSON.stringify(file));
  await fs.promises.rename(tmp, filePath);
  await syncDirectory(filePath);
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
  options: SnapshotLoadOptions = {}
): Promise<number> {
  const { clearFirst = true, missingOk = true, metadata } = options;
  let raw: string;
  try {
    raw = await fs.promises.readFile(filePath, 'utf8');
  } catch (err) {
    if (missingOk && (err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
    throw err;
  }
  const file = JSON.parse(raw) as SnapshotFile;
  if (
    !file ||
    typeof file !== 'object' ||
    (file.version !== 1 && file.version !== 2) ||
    !Array.isArray(file.entries) ||
    (file.aofSeq !== undefined && (!Number.isSafeInteger(file.aofSeq) || file.aofSeq < 0))
  ) {
    throw new Error(`invalid snapshot file: ${filePath}`);
  }
  if (metadata) metadata.aofSeq = file.aofSeq;
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
    case 'transaction':
      cache.atomic(() => {
        for (const child of op.ops) applyAofOp(cache, child);
      });
      break;
  }
}

interface AofRecord {
  version: 1;
  seq: number;
  op: AofOp;
}

interface ParsedAofLine {
  seq?: number;
  op: AofOp;
}

function parseAofLine(line: string): ParsedAofLine | undefined {
  const parsed = JSON.parse(line) as Partial<AofRecord> & AofOp;
  if (
    parsed &&
    typeof parsed === 'object' &&
    Number.isSafeInteger(parsed.seq) &&
    (parsed.seq as number) >= 0 &&
    parsed.op &&
    typeof parsed.op === 'object'
  ) {
    return { seq: parsed.seq as number, op: parsed.op as AofOp };
  }
  return { op: parsed as AofOp };
}

/**
 * Append-only mutation log. Each record has a durable sequence number so a
 * snapshot can identify which AOF records it already contains.
 */
export class AofLog {
  private filePath: string | undefined;
  private lastSeq = 0;
  private rotationCounter = 0;

  constructor(filePath?: string) {
    this.filePath = filePath;
    if (filePath) ensureDir(filePath);
    this.lastSeq = this.readLastSequence();
  }

  get enabled(): boolean {
    return this.filePath !== undefined;
  }

  get path(): string | undefined {
    return this.filePath;
  }

  /** Highest sequence number currently assigned to an AOF record. */
  get sequence(): number {
    return this.lastSeq;
  }

  append(op: AofOp): void {
    if (!this.filePath) return;
    if (this.lastSeq >= Number.MAX_SAFE_INTEGER) throw new Error('AOF sequence exhausted');
    const record: AofRecord = { version: 1, seq: this.lastSeq + 1, op };
    fs.appendFileSync(this.filePath, JSON.stringify(record) + '\n', 'utf8');
    this.lastSeq = record.seq;
  }

  private readLastSequence(): number {
    if (!this.filePath) return 0;
    let raw: string;
    try {
      raw = fs.readFileSync(this.filePath, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
      throw err;
    }
    let max = 0;
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const parsed = parseAofLine(trimmed);
        if (parsed?.seq !== undefined && parsed.seq > max) max = parsed.seq;
      } catch {
        // Replay handles corrupt lines; they do not contribute a sequence.
      }
    }
    return max;
  }

  /** Replay the log, skipping records already represented by a snapshot. */
  async replay(cache: KVCache, snapshotSeq?: number): Promise<number> {
    if (!this.filePath) return 0;
    if (snapshotSeq !== undefined) checkAofSeq(snapshotSeq, 'snapshotSeq');
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
        const parsed = parseAofLine(trimmed);
        if (!parsed) continue;
        if (parsed.seq !== undefined && parsed.seq > this.lastSeq) this.lastSeq = parsed.seq;
        // A v2 snapshot covers all legacy unsequenced records that existed
        // when it was written. New records always carry a sequence number.
        if (snapshotSeq !== undefined && (parsed.seq === undefined || parsed.seq <= snapshotSeq)) continue;
        applyAofOp(cache, parsed.op);
        applied++;
      } catch {
        // Skip corrupt lines (e.g. torn trailing write) and continue.
      }
    }
    if (snapshotSeq !== undefined && snapshotSeq > this.lastSeq) this.lastSeq = snapshotSeq;
    return applied;
  }

  /**
   * Atomically replace the log with records newer than a committed snapshot.
   * The old file stays in place until the replacement is ready, so a crash
   * before rename leaves a replayable log and a crash after rename leaves only
   * the post-snapshot tail.
   */
  rotateAfter(snapshotSeq: number): void {
    if (!this.filePath) return;
    checkAofSeq(snapshotSeq, 'snapshotSeq');
    let raw: string;
    try {
      raw = fs.readFileSync(this.filePath, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') raw = '';
      else throw err;
    }
    const tail: string[] = [];
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const parsed = parseAofLine(trimmed);
        if (parsed?.seq !== undefined && parsed.seq > snapshotSeq) tail.push(trimmed);
      } catch {
        // Drop corrupt lines while compacting; replay already ignores them.
      }
    }
    const tmp = `${this.filePath}.rotate.${process.pid}.${this.rotationCounter++}`;
    const fd = fs.openSync(tmp, 'w');
    try {
      fs.writeFileSync(fd, tail.length === 0 ? '' : `${tail.join('\n')}\n`, 'utf8');
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(tmp, this.filePath);
    const dirFd = fs.openSync(path.dirname(this.filePath), 'r');
    try {
      fs.fsyncSync(dirFd);
    } finally {
      fs.closeSync(dirFd);
    }
    this.lastSeq = Math.max(snapshotSeq, this.readLastSequence());
  }

  /** Backward-compatible alias for callers that only need an empty AOF. */
  async truncate(): Promise<void> {
    this.rotateAfter(this.lastSeq);
  }
}
