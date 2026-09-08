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

const CRLF = '\r\n';

export function encodeSimple(s: string): Buffer {
  return Buffer.from(`+${s}${CRLF}`, 'utf8');
}

export function encodeError(message: string): Buffer {
  const oneLine = String(message).split('\r\n')[0];
  return Buffer.from(`-${oneLine}${CRLF}`, 'utf8');
}

export function encodeInt(n: number): Buffer {
  if (!Number.isInteger(n)) throw new Error('RESP integers must be integers');
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
export function encodeCommand(args: Array<string | number>): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}${CRLF}`, 'utf8')];
  for (const a of args) parts.push(encodeBulk(String(a)));
  return Buffer.concat(parts);
}

function readLine(buf: Buffer, pos: number): [string, number] | null {
  const end = buf.indexOf('\r\n', pos);
  if (end === -1) return null;
  return [buf.toString('utf8', pos, end), end + 2];
}

/**
 * Parse one value starting at pos.
 * Returns [reply, nextPos], or null when more bytes are needed.
 * Throws on malformed input.
 */
function parseValue(buf: Buffer, pos: number): [RespReply, number] | null {
  if (pos >= buf.length) return null;
  const prefix = buf[pos];
  if (prefix === 0x2b /* + */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    return [{ kind: 'simple', value: line[0] }, line[1]];
  }
  if (prefix === 0x2d /* - */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    return [{ kind: 'error', message: line[0] }, line[1]];
  }
  if (prefix === 0x3a /* : */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    const n = parseInt(line[0], 10);
    if (!Number.isFinite(n)) throw new Error(`malformed RESP integer: ${line[0]}`);
    return [{ kind: 'int', value: n }, line[1]];
  }
  if (prefix === 0x24 /* $ */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    const len = parseInt(line[0], 10);
    if (len === -1) return [{ kind: 'bulk', value: null }, line[1]];
    if (!Number.isInteger(len) || len < -1) throw new Error(`malformed RESP bulk length: ${line[0]}`);
    if (line[1] + len + 2 > buf.length) return null;
    const value = buf.toString('utf8', line[1], line[1] + len);
    if (buf[line[1] + len] !== 0x0d || buf[line[1] + len + 1] !== 0x0a) {
      throw new Error('malformed RESP bulk terminator');
    }
    return [{ kind: 'bulk', value }, line[1] + len + 2];
  }
  if (prefix === 0x2a /* * */) {
    const line = readLine(buf, pos + 1);
    if (!line) return null;
    const count = parseInt(line[0], 10);
    if (count === -1) return [{ kind: 'nil' }, line[1]];
    if (!Number.isInteger(count) || count < -1) throw new Error(`malformed RESP array length: ${line[0]}`);
    const items: Array<RespReply | null> = [];
    let p = line[1];
    for (let i = 0; i < count; i++) {
      const parsed = parseValue(buf, p);
      if (!parsed) return null;
      items.push(parsed[0]);
      p = parsed[1];
    }
    return [{ kind: 'array', items }, p];
  }
  throw new Error(`malformed RESP prefix byte: 0x${prefix.toString(16)}`);
}

/** Incremental streaming decoder: push bytes, drain complete values. */
export class RespDecoder {
  private buf: Buffer = Buffer.alloc(0);

  push(chunk: Buffer | string): RespReply[] {
    const piece = typeof chunk === 'string' ? Buffer.from(chunk, 'utf8') : chunk;
    this.buf = this.buf.length === 0 ? piece : Buffer.concat([this.buf, piece]);
    const out: RespReply[] = [];
    for (;;) {
      if (this.buf.length === 0) break;
      let parsed: [RespReply, number] | null;
      try {
        parsed = parseValue(this.buf, 0);
      } catch (err) {
        this.buf = Buffer.alloc(0);
        throw err;
      }
      if (!parsed) break;
      out.push(parsed[0]);
      this.buf = this.buf.slice(parsed[1]);
    }
    return out;
  }

  get bufferedBytes(): number {
    return this.buf.length;
  }

  reset(): void {
    this.buf = Buffer.alloc(0);
  }
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
