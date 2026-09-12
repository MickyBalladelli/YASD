// Minimal RESP2-subset codec for the YASD wire protocol.
// Requests: arrays of bulk strings (`*2 $3 GET $3 foo`).
// Replies: simple strings (+), errors (-), integers (:), bulk strings ($,
// `$-1` = nil), arrays (*, `*-1` = nil). Cache values travel as JSON bulk
// strings so objects/arrays survive the round trip.

export type RespReply =
  | { kind: 'simple'; value: string }
  | { kind: 'error'; message: string }
  | { kind: 'int'; value: number }
  | { kind: 'bulk'; value: string | null }
  | { kind: 'array'; items: Array<RespReply | null> }
  | { kind: 'nil' };

import { RespReader } from './resp-reader';
const CRLF = '\r\n';

export interface RespDecoderOptions {
  /** Maximum bytes in one complete RESP value. Default 8 MiB. */
  maxFrameBytes?: number;
  /** Maximum items in any RESP array. Default 1024. */
  maxArguments?: number;
  /** Maximum bulk-string payload bytes. Default 4 MiB. */
  maxBulkBytes?: number;
  /** Maximum nested array depth. Default 32. */
  maxDepth?: number;
  /** Maximum unparsed bytes retained between pushes. Default 8 MiB. */
  maxBufferedBytes?: number;
}

interface RespLimits {
  maxFrameBytes: number;
  maxArguments: number;
  maxBulkBytes: number;
  maxDepth: number;
  maxBufferedBytes: number;
}

export const DEFAULT_RESP_LIMITS: Readonly<RespLimits> = Object.freeze({
  maxFrameBytes: 8 * 1024 * 1024,
  maxArguments: 1024,
  maxBulkBytes: 4 * 1024 * 1024,
  maxDepth: 32,
  maxBufferedBytes: 8 * 1024 * 1024,
});

/** Validate response shape and size without allocating its encoded buffer. */
export function replyByteLength(reply: RespReply | null, depth = 0): number {
  let bytes: number;
  if (reply === null || reply.kind === 'nil') return 5;
  switch (reply.kind) {
    case 'bulk': {
      if (reply.value === null) return 5;
      const size = Buffer.byteLength(reply.value);
      if (size > DEFAULT_RESP_LIMITS.maxBulkBytes) throw new Error('RESP bulk length exceeds limit');
      bytes = size + String(size).length + 5;
      break;
    }
    case 'array':
      if (depth >= DEFAULT_RESP_LIMITS.maxDepth || reply.items.length > DEFAULT_RESP_LIMITS.maxArguments) {
        throw new Error('RESP array/depth exceeds limit');
      }
      bytes = String(reply.items.length).length + 3;
      for (const item of reply.items) {
        bytes += replyByteLength(item, depth + 1);
        if (bytes > DEFAULT_RESP_LIMITS.maxFrameBytes) throw new Error('RESP frame exceeds limit');
      }
      break;
    case 'int':
      if (!Number.isSafeInteger(reply.value)) throw new Error('RESP integers must be safe integers');
      bytes = String(reply.value).length + 3;
      break;
    case 'simple': bytes = Buffer.byteLength(reply.value) + 3; break;
    case 'error': bytes = Buffer.byteLength(reply.message) + 3; break;
  }
  if (bytes > DEFAULT_RESP_LIMITS.maxFrameBytes) throw new Error('RESP frame exceeds limit');
  return bytes;
}

function positiveLimit(value: number | undefined, name: string, fallback: number): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new Error(`${name} must be a safe integer >= 1`);
  }
  return limit;
}

function resolveLimits(options: RespDecoderOptions): RespLimits {
  return {
    maxFrameBytes: positiveLimit(options.maxFrameBytes, 'maxFrameBytes', DEFAULT_RESP_LIMITS.maxFrameBytes),
    maxArguments: positiveLimit(options.maxArguments, 'maxArguments', DEFAULT_RESP_LIMITS.maxArguments),
    maxBulkBytes: positiveLimit(options.maxBulkBytes, 'maxBulkBytes', DEFAULT_RESP_LIMITS.maxBulkBytes),
    maxDepth: positiveLimit(options.maxDepth, 'maxDepth', DEFAULT_RESP_LIMITS.maxDepth),
    maxBufferedBytes: positiveLimit(options.maxBufferedBytes, 'maxBufferedBytes', DEFAULT_RESP_LIMITS.maxBufferedBytes),
  };
}

function checkFrameBytes(frameStart: number, pos: number, limits: RespLimits): void {
  if (pos - frameStart > limits.maxFrameBytes) {
    throw new Error(`RESP frame exceeds ${limits.maxFrameBytes} bytes`);
  }
}

export function encodeSimple(s: string): Buffer {
  return Buffer.from(`+${s}${CRLF}`, 'utf8');
}

export function encodeError(message: string): Buffer {
  const oneLine = String(message).replace(/[\r\n]/g, ' ');
  return Buffer.from(`-${oneLine}${CRLF}`, 'utf8');
}

export function encodeInt(n: number): Buffer {
  if (!Number.isSafeInteger(n)) throw new Error('RESP integers must be safe integers');
  return Buffer.from(`:${n}${CRLF}`, 'utf8');
}

export function encodeBulk(s: string | null | undefined): Buffer {
  if (s === null || s === undefined) return Buffer.from('$-1\r\n', 'utf8');
  const body = Buffer.from(s, 'utf8');
  return Buffer.concat([Buffer.from(`$${body.length}${CRLF}`, 'utf8'), body, Buffer.from(CRLF, 'utf8')]);
}

export function encodeArray(items: Array<RespReply | null>): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${items.length}${CRLF}`, 'utf8')];
  for (const item of items) parts.push(encodeReply(item));
  return Buffer.concat(parts);
}

export function encodeReply(reply: RespReply | null): Buffer {
  if (reply === null) return Buffer.from('$-1\r\n', 'utf8');
  switch (reply.kind) {
    case 'simple':
      return encodeSimple(reply.value);
    case 'error':
      return encodeError(reply.message);
    case 'int':
      return encodeInt(reply.value);
    case 'bulk':
      return encodeBulk(reply.value);
    case 'array':
      return encodeArray(reply.items);
    case 'nil':
      return Buffer.from('*-1\r\n', 'utf8');
  }
}

/** Encode a command as an array of bulk strings. */
export function commandByteLength(args: Array<string | number>): number {
  return replyByteLength({ kind: 'array', items: args.map(value => ({ kind: 'bulk', value: String(value) })) });
}

export function encodeCommand(args: Array<string | number>): Buffer {
  commandByteLength(args);
  const parts: Buffer[] = [Buffer.from(`*${args.length}${CRLF}`, 'utf8')];
  for (const a of args) parts.push(encodeBulk(String(a)));
  return Buffer.concat(parts);
}

function readLine(buf: Buffer, pos: number): [string, number] | null {
  const end = buf.indexOf('\r\n', pos);
  if (end === -1) return null;
  return [decodeUtf8(buf.subarray(pos, end)), end + 2];
}

function decodeUtf8(value: Buffer): string {
  const text = value.toString('utf8');
  if (!Buffer.from(text, 'utf8').equals(value)) {
    throw new Error('invalid RESP UTF-8');
  }
  return text;
}

function parseRespInteger(line: string, kind: string): number {
  if (!/^-?\d+$/.test(line)) {
    throw new Error(`malformed RESP ${kind}: ${line}`);
  }
  const value = Number(line);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`malformed RESP ${kind}: ${line}`);
  }
  return value;
}

/**
 * Parse one value starting at pos.
 * Returns [reply, nextPos], or null when more bytes are needed.
 * Throws on malformed input.
 */
function parseValue(
  buf: Buffer,
  pos: number,
  limits: RespLimits,
  depth: number,
  frameStart: number
): [RespReply, number] | null {
  if (pos >= buf.length) return null;
  checkFrameBytes(frameStart, pos, limits);
  const prefix = buf[pos];
  if (prefix === 0x2b /* + */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    checkFrameBytes(frameStart, line[1], limits);
    return [{ kind: 'simple', value: line[0] }, line[1]];
  }
  if (prefix === 0x2d /* - */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    checkFrameBytes(frameStart, line[1], limits);
    return [{ kind: 'error', message: line[0] }, line[1]];
  }
  if (prefix === 0x3a /* : */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    checkFrameBytes(frameStart, line[1], limits);
    const n = parseRespInteger(line[0], 'integer');
    return [{ kind: 'int', value: n }, line[1]];
  }
  if (prefix === 0x24 /* $ */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    checkFrameBytes(frameStart, line[1], limits);
    const len = parseRespInteger(line[0], 'bulk length');
    if (len === -1) return [{ kind: 'bulk', value: null }, line[1]];
    if (!Number.isInteger(len) || len < -1) throw new Error(`malformed RESP bulk length: ${line[0]}`);
    if (len > limits.maxBulkBytes) {
      throw new Error(`RESP bulk length exceeds ${limits.maxBulkBytes} bytes`);
    }
    checkFrameBytes(frameStart, line[1] + len + 2, limits);
    if (line[1] + len + 2 > buf.length) return null;
    const value = decodeUtf8(buf.subarray(line[1], line[1] + len));
    if (buf[line[1] + len] !== 0x0d || buf[line[1] + len + 1] !== 0x0a) {
      throw new Error('malformed RESP bulk terminator');
    }
    return [{ kind: 'bulk', value }, line[1] + len + 2];
  }
  if (prefix === 0x2a /* * */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    checkFrameBytes(frameStart, line[1], limits);
    if (depth >= limits.maxDepth) {
      throw new Error(`RESP nesting exceeds depth ${limits.maxDepth}`);
    }
    const count = parseRespInteger(line[0], 'array length');
    if (count === -1) return [{ kind: 'nil' }, line[1]];
    if (!Number.isInteger(count) || count < -1) throw new Error(`malformed RESP array length: ${line[0]}`);
    if (count > limits.maxArguments) {
      throw new Error(`RESP array length exceeds ${limits.maxArguments} items`);
    }
    const items: Array<RespReply | null> = [];
    let p = line[1];
    for (let i = 0; i < count; i++) {
      const parsed = parseValue(buf, p, limits, depth + 1, frameStart);
      if (!parsed) return null;
      items.push(parsed[0]);
      p = parsed[1];
      checkFrameBytes(frameStart, p, limits);
    }
    return [{ kind: 'array', items }, p];
  }
  throw new Error(`malformed RESP prefix byte: 0x${prefix.toString(16)}`);
}

/** Incremental streaming decoder: push bytes, drain complete values. */
export class RespDecoder extends RespReader {
  constructor(options: RespDecoderOptions = {}) { super(resolveLimits(options)); }
}

/** Extract command argv (array of strings) from a decoded request. */
export function requestArgv(request: RespReply): string[] {
  if (request.kind !== 'array') throw new Error('protocol error: expected an array command');
  const argv: string[] = [];
  for (const item of request.items) {
    if (!item || item.kind !== 'bulk' || item.value === null) {
      throw new Error('protocol error: command args must be bulk strings');
    }
    argv.push(item.value);
  }
  if (argv.length === 0) throw new Error('protocol error: empty command');
  return argv;
}
