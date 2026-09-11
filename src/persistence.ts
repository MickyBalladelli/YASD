// Persistence for YASD cache: snapshots + append-only log (AOF).
// A cache stays ephemeral by design — this exists for restart-storm
// protection: SAVE/LOAD a full snapshot, replay the AOF on startup.

import * as fs from 'fs';
import * as path from 'path';
import { isIncompleteJson } from './json-prefix';
import { DatabaseError } from './errors';
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

export type AofRecoveryState = 'clean' | 'torn-tail' | 'corrupt'

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
  replace(entries: SnapshotEntry[]): number;
}

/** One replayable mutation inside an AOF transaction. */
export type AofMutation =
  | { op: 'set'; key: string; value: SnapshotEntry['value']; ttlMs?: number; expiresAt?: number | null }
  | { op: 'mset'; entries: AofBatchEntry[] }
  | { op: 'del'; keys: string[] }
  | { op: 'clear'; prefix: string }
  | { op: 'expire'; key: string; ttlMs?: number; expiresAt?: number }
  | { op: 'persist'; key: string }
  | { op: 'incr'; key: string; by: number }
  | { op: 'patch'; entries: SnapshotEntry[]; deleted: string[] };

export type AofBatchEntry = KVBatchEntry & {
  /** null = persistent, number = exact absolute expiry deadline. */
  expiresAt?: number | null;
};

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

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function hasOwn(value: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key)
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function isAofMutation(value: unknown): value is AofMutation {
  if (!isRecord(value) || typeof value.op !== 'string') return false
  switch (value.op) {
    case 'set':
      return (
        typeof value.key === 'string' &&
        hasOwn(value, 'value') &&
        (!hasOwn(value, 'ttlMs') || isFiniteNumber(value.ttlMs)) &&
        (!hasOwn(value, 'expiresAt') || value.expiresAt === null || isFiniteNumber(value.expiresAt))
      )
    case 'mset':
      return (
        Array.isArray(value.entries) &&
        value.entries.every(entry =>
          isRecord(entry) &&
          typeof entry.key === 'string' &&
          hasOwn(entry, 'value') &&
          (!hasOwn(entry, 'ttlMs') || isFiniteNumber(entry.ttlMs)) &&
          (!hasOwn(entry, 'expiresAt') || entry.expiresAt === null || isFiniteNumber(entry.expiresAt))
        )
      )
    case 'del':
      return Array.isArray(value.keys) && value.keys.every(key => typeof key === 'string')
    case 'clear':
      return typeof value.prefix === 'string'
    case 'expire':
      return (
        typeof value.key === 'string' &&
        ((hasOwn(value, 'expiresAt') && isFiniteNumber(value.expiresAt)) ||
          (hasOwn(value, 'ttlMs') && isFiniteNumber(value.ttlMs)))
      )
    case 'patch':
      return Array.isArray(value.entries) && Array.isArray(value.deleted) &&
        value.deleted.every(key => typeof key === 'string') &&
        value.entries.every(entry => isRecord(entry) && typeof entry.key === 'string' &&
          hasOwn(entry, 'value') && (!hasOwn(entry, 'expiresAt') || isFiniteNumber(entry.expiresAt)))
    case 'persist':
      return typeof value.key === 'string'
    case 'incr':
      return typeof value.key === 'string' && isFiniteNumber(value.by)
    default:
      return false
  }
}

function validateAofOp(value: unknown): asserts value is AofOp {
  if (
    isRecord(value) &&
    value.op === 'transaction' &&
    Array.isArray(value.ops) &&
    value.ops.every(entry => isAofMutation(entry))
  ) {
    return
  }
  if (!isAofMutation(value)) throw new Error('invalid AOF operation')
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
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
  const count = clearFirst ? store.replace(file.entries) : store.restore(file.entries);
  if (metadata) metadata.aofSeq = file.aofSeq;
  return count;
}

/** Apply one AOF op without producing further log output. */
export function applyAofOp(cache: KVCache, op: AofOp): void {
  switch (op.op) {
    case 'set':
      if (Object.prototype.hasOwnProperty.call(op, 'expiresAt')) {
        cache.setAt(op.key, op.value, op.expiresAt === null ? undefined : op.expiresAt);
      } else {
        cache.set(op.key, op.value, op.ttlMs);
      }
      break;
    case 'mset':
      for (const entry of op.entries) {
        if (Object.prototype.hasOwnProperty.call(entry, 'expiresAt')) {
          cache.setAt(entry.key, entry.value, entry.expiresAt === null ? undefined : entry.expiresAt);
        } else {
          cache.set(entry.key, entry.value, entry.ttlMs);
        }
      }
      break;
    case 'del':
      for (const key of op.keys) cache.del(key);
      break;
    case 'clear':
      cache.clearPrefix(op.prefix);
      break;
    case 'expire':
      if (Object.prototype.hasOwnProperty.call(op, 'expiresAt')) {
        cache.expireAt(op.key, op.expiresAt as number);
      } else if (op.ttlMs !== undefined) {
        cache.expire(op.key, op.ttlMs);
      }
      break;
    case 'persist':
      cache.persist(op.key);
      break;
    case 'incr':
      cache.incr(op.key, op.by);
      break;
    case 'patch':
      cache.atomic(() => {
        for (const key of op.deleted) cache.del(key);
        for (const entry of op.entries) cache.setAt(entry.key, entry.value, entry.expiresAt);
      });
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

function parseAofLine(line: string): ParsedAofLine {
  const parsed: unknown = JSON.parse(line)
  if (!isRecord(parsed)) throw new Error('AOF record must be an object')
  if (hasOwn(parsed, 'version') || hasOwn(parsed, 'seq') || isRecord(parsed.op)) {
    if (parsed.version !== 1) throw new Error('unsupported AOF record version')
    if (!Number.isSafeInteger(parsed.seq) || (parsed.seq as number) < 0) {
      throw new Error('invalid AOF record sequence')
    }
    validateAofOp(parsed.op)
    return { seq: parsed.seq as number, op: parsed.op }
  }
  validateAofOp(parsed)
  return { op: parsed }
}

/**
 * Append-only mutation log. Each record has a durable sequence number so a
 * snapshot can identify which AOF records it already contains.
 */
export class AofLog {
  private filePath: string | undefined;
  private lastSeq = 0;
  private rotationCounter = 0;
  private recoveryStatus: AofRecoveryState = 'clean'
  private recoveryDetail?: string

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

  get recoveryState(): AofRecoveryState {
    return this.recoveryStatus
  }

  get recoveryError(): string | undefined {
    return this.recoveryDetail
  }

  append(op: AofOp): void {
    if (!this.filePath) return;
    if (this.recoveryStatus === 'corrupt') {
      throw new Error(`AOF is corrupt: ${this.recoveryDetail ?? 'repair required before writing'}`)
    }
    validateAofOp(op)
    if (this.lastSeq >= Number.MAX_SAFE_INTEGER) throw new Error('AOF sequence exhausted');
    const record: AofRecord = { version: 1, seq: this.lastSeq + 1, op };
    const encoded = JSON.stringify(record) + '\n';
    try {
      fs.appendFileSync(this.filePath, encoded, 'utf8');
    } catch (error) {
      // A failed append may have written a prefix. Never append past that prefix.
      this.recoveryStatus = 'corrupt';
      this.recoveryDetail = 'AOF append failed; reopen and repair before writing';
      throw error;
    }
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
    let previousSeq: number | undefined
    const lines = raw.split('\n')
    for (const [i, line] of lines.entries()) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (raw.length === 0 || (i === lines.length - 1 && raw.endsWith('\n'))) continue
        this.noteRecovery(lines, i, new Error('empty AOF record'), raw)
        break
      }
      try {
        const parsed = parseAofLine(trimmed);
        if (parsed.seq !== undefined) {
          if (previousSeq !== undefined && parsed.seq <= previousSeq) {
            throw new Error(`AOF sequence is not increasing (${parsed.seq} after ${previousSeq})`)
          }
          previousSeq = parsed.seq
          if (parsed.seq > max) max = parsed.seq
        }
      } catch (error) {
        this.noteRecovery(lines, i, error, raw)
        break
      }
    }
    return max;
  }

  private noteRecovery(lines: string[], index: number, error: unknown, raw: string): void {
    const lineNumber = index + 1
    const detail = `AOF recovery at line ${lineNumber}: ${errorMessage(error)}`
    const isTornTail = index === lines.length - 1 && error instanceof SyntaxError && isIncompleteJson(lines[index])
    if (isTornTail) {
      if (this.recoveryStatus === 'clean') {
        this.recoveryStatus = 'torn-tail'
        this.recoveryDetail = detail
      }
      this.repairTornTail(raw)
      return
    }
    this.recoveryStatus = 'corrupt'
    this.recoveryDetail = detail
  }

  private repairTornTail(raw: string): void {
    if (!this.filePath) return;
    const lastNewline = raw.lastIndexOf('\n')
    const safeBytes = Buffer.byteLength(raw.slice(0, lastNewline + 1), 'utf8')
    try {
      fs.truncateSync(this.filePath, safeBytes)
    } catch (error) {
      this.recoveryStatus = 'corrupt'
      this.recoveryDetail = `AOF torn-tail repair failed: ${errorMessage(error)}`
    }
  }

  /** Replay the log; stop at corruption and only tolerate a torn final line. */
  async replay(cache: KVCache, snapshotSeq?: number): Promise<number> {
    if (!this.filePath) return 0;
    if (snapshotSeq !== undefined) {
      checkAofSeq(snapshotSeq, 'snapshotSeq');
      this.lastSeq = Math.max(this.lastSeq, snapshotSeq);
    }
    let raw: string;
    try {
      raw = await fs.promises.readFile(this.filePath, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
      throw err;
    }
    let applied = 0;
    let previousSeq: number | undefined
    const lines = raw.split('\n')
    for (const [i, line] of lines.entries()) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (raw.length === 0 || (i === lines.length - 1 && raw.endsWith('\n'))) continue
        this.noteRecovery(lines, i, new Error('empty AOF record'), raw)
        break
      }
      try {
        const parsed = parseAofLine(trimmed);
        if (parsed.seq !== undefined) {
          if (previousSeq !== undefined && parsed.seq <= previousSeq) {
            throw new Error(`AOF sequence is not increasing (${parsed.seq} after ${previousSeq})`)
          }
          previousSeq = parsed.seq
          if (parsed.seq > this.lastSeq) this.lastSeq = parsed.seq
        }
        // A v2 snapshot covers all legacy unsequenced records that existed
        // when it was written. New records always carry a sequence number.
        if (snapshotSeq !== undefined && (parsed.seq === undefined || parsed.seq <= snapshotSeq)) continue;
        cache.atomic(() => applyAofOp(cache, parsed.op));
        applied++;
      } catch (error) {
        this.noteRecovery(lines, i, error, raw)
        break
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
    if (this.recoveryStatus === 'corrupt') {
      throw new Error(`AOF is corrupt: ${this.recoveryDetail ?? 'repair required before rotation'}`)
    }
    checkAofSeq(snapshotSeq, 'snapshotSeq');
    let raw: string;
    try {
      raw = fs.readFileSync(this.filePath, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') raw = '';
      else throw err;
    }
    const tail: string[] = [];
    let previousSeq: number | undefined
    const lines = raw.split('\n')
    for (const [i, line] of lines.entries()) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (raw.length === 0 || (i === lines.length - 1 && raw.endsWith('\n'))) continue
        this.noteRecovery(lines, i, new Error('empty AOF record'), raw)
        if (this.recoveryState === 'corrupt') throw new Error(this.recoveryError)
        break
      }
      try {
        const parsed = parseAofLine(trimmed);
        if (parsed.seq !== undefined) {
          if (previousSeq !== undefined && parsed.seq <= previousSeq) {
            throw new Error(`AOF sequence is not increasing (${parsed.seq} after ${previousSeq})`)
          }
          previousSeq = parsed.seq
          if (parsed.seq > snapshotSeq) tail.push(trimmed)
        }
      } catch (error) {
        this.noteRecovery(lines, i, error, raw)
        if (this.recoveryState === 'corrupt') {
          throw new Error(this.recoveryError)
        }
        break
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
