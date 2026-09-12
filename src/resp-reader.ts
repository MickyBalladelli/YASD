import { TextDecoder } from "util";
import { DatabaseError } from "./errors";
import type { RespReply } from "./protocol";

export interface ReaderLimits {
  maxFrameBytes: number;
  maxArguments: number;
  maxBulkBytes: number;
  maxDepth: number;
  maxBufferedBytes: number;
}
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
/** Resumable RESP state machine. Each input byte is consumed once. */
export class RespReader {
  private frameBytes = 0;
  private prefix?: number;
  private line = Buffer.allocUnsafe(128);
  private lineLength = 0;
  private lineCR = false;
  private bulk?: { buffer: Buffer; read: number; terminator: number };
  private arrays: Array<{ remaining: number; items: RespReply[] }> = [];
  constructor(private readonly limits: ReaderLimits) {}
  get bufferedBytes(): number {
    return this.frameBytes;
  }
  get retainedBytes(): number {
    return (
      this.frameBytes +
      (this.bulk ? this.bulk.buffer.length - this.bulk.read : 0) +
      this.line.length
    );
  }
  reset(): void {
    this.frameBytes = 0;
    this.prefix = undefined;
    this.lineLength = 0;
    this.lineCR = false;
    this.bulk = undefined;
    this.arrays = [];
    this.line = Buffer.allocUnsafe(128);
  }
  private count(bytes: number): void {
    this.frameBytes += bytes;
    if (this.frameBytes > this.limits.maxFrameBytes)
      throw new Error(`RESP frame exceeds ${this.limits.maxFrameBytes} bytes`);
  }
  private emit(value: RespReply, out: RespReply[]): void {
    for (;;) {
      const parent = this.arrays[this.arrays.length - 1];
      if (!parent) {
        out.push(value);
        this.frameBytes = 0;
        return;
      }
      parent.items.push(value);
      if (--parent.remaining > 0) return;
      this.arrays.pop();
      value = { kind: "array", items: parent.items };
    }
  }
  private header(out: RespReply[]): void {
    const text = utf8.decode(this.line.subarray(0, this.lineLength));
    const prefix = this.prefix;
    this.prefix = undefined;
    this.lineLength = 0;
    this.lineCR = false;
    if (prefix === 43) {
      this.emit({ kind: "simple", value: text }, out);
      return;
    }
    if (prefix === 45) {
      this.emit({ kind: "error", message: text }, out);
      return;
    }
    const n = Number(text);
    if (!/^-?\d+$/.test(text) || !Number.isSafeInteger(n))
      throw new Error("malformed RESP integer/length");
    if (prefix === 58) {
      this.emit({ kind: "int", value: n }, out);
      return;
    }
    if (prefix === 36) {
      if (n === -1) {
        this.emit({ kind: "bulk", value: null }, out);
        return;
      }
      if (n < 0) throw new Error("malformed RESP bulk length");
      if (n > this.limits.maxBulkBytes)
        throw new Error(
          `RESP bulk length exceeds ${this.limits.maxBulkBytes} bytes`,
        );
      if (this.frameBytes + n + 2 > this.limits.maxFrameBytes)
        throw new Error(
          `RESP frame exceeds ${this.limits.maxFrameBytes} bytes`,
        );
      if (this.frameBytes + n + 2 > this.limits.maxBufferedBytes)
        throw new Error(
          `RESP buffered bytes exceed ${this.limits.maxBufferedBytes}`,
        );
      this.bulk = { buffer: Buffer.allocUnsafe(n), read: 0, terminator: 0 };
      return;
    }
    if (this.arrays.length >= this.limits.maxDepth)
      throw new Error(`RESP nesting exceeds depth ${this.limits.maxDepth}`);
    if (n < -1) throw new Error("malformed RESP array length");
    if (n > this.limits.maxArguments)
      throw new Error(
        `RESP array length exceeds ${this.limits.maxArguments} items`,
      );
    if (n === -1) this.emit({ kind: "nil" }, out);
    else if (n === 0) this.emit({ kind: "array", items: [] }, out);
    else this.arrays.push({ remaining: n, items: [] });
  }
  push(chunk: Buffer | string): RespReply[] {
    const bytes = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
    const out: RespReply[] = [];
    try {
      if (this.frameBytes + bytes.length > this.limits.maxBufferedBytes)
        throw new Error(
          `RESP buffered bytes exceed ${this.limits.maxBufferedBytes}`,
        );
      let pos = 0;
      while (pos < bytes.length) {
        if (this.bulk) {
          const bulk = this.bulk;
          const size = Math.min(
            bulk.buffer.length - bulk.read,
            bytes.length - pos,
          );
          if (size) {
            bytes.copy(bulk.buffer, bulk.read, pos, pos + size);
            bulk.read += size;
            pos += size;
            this.count(size);
          }
          if (bulk.read < bulk.buffer.length || pos === bytes.length) continue;
          const expected = bulk.terminator === 0 ? 13 : 10;
          if (bytes[pos++] !== expected)
            throw new Error("malformed RESP bulk terminator");
          this.count(1);
          if (++bulk.terminator === 2) {
            let value: string;
            try {
              value = utf8.decode(bulk.buffer);
            } catch {
              throw new Error("invalid RESP UTF-8");
            }
            this.bulk = undefined;
            this.emit({ kind: "bulk", value }, out);
          }
          continue;
        }
        const byte = bytes[pos++];
        this.count(1);
        if (this.prefix === undefined) {
          if (![43, 45, 58, 36, 42].includes(byte))
            throw new Error("malformed RESP prefix byte");
          this.prefix = byte;
          continue;
        }
        if (this.lineCR) {
          if (byte !== 10) throw new Error("malformed RESP line terminator");
          this.header(out);
          continue;
        }
        if (byte === 13) {
          this.lineCR = true;
          continue;
        }
        if (this.lineLength === this.line.length) {
          const bigger = Buffer.allocUnsafe(
            Math.min(this.line.length * 2, this.limits.maxFrameBytes),
          );
          this.line.copy(bigger);
          this.line = bigger;
        }
        this.line[this.lineLength++] = byte;
      }
      return out;
    } catch (error) {
      this.reset();
      throw new DatabaseError(
        error instanceof Error ? error.message : "invalid RESP input",
        "PROTOCOL_ERROR",
        { cause: error },
      );
    }
  }
}
