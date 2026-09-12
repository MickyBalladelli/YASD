// RESP2 subset. Requests are bulk-string arrays; cache values are JSON bulk strings.
import { RespReader } from "./resp-reader";
import { DatabaseError } from "./errors";

export type RespReply =
  | { kind: "simple"; value: string }
  | { kind: "error"; message: string }
  | { kind: "int"; value: number }
  | { kind: "bulk"; value: string | null }
  | { kind: "array"; items: Array<RespReply | null> }
  | { kind: "nil" };

export interface RespDecoderOptions {
  maxFrameBytes?: number;
  maxArguments?: number;
  maxBulkBytes?: number;
  maxDepth?: number;
  maxBufferedBytes?: number;
}
export const DEFAULT_RESP_LIMITS = Object.freeze({
  maxFrameBytes: 8 * 1024 * 1024,
  maxArguments: 1024,
  maxBulkBytes: 4 * 1024 * 1024,
  maxDepth: 32,
  maxBufferedBytes: 8 * 1024 * 1024,
});
const invalid = (message: string): never => {
  throw new DatabaseError(message, "PROTOCOL_ERROR");
};

/** Preflight reply size before allocating its encoded buffers. */
export function replyByteLength(reply: RespReply | null, depth = 0): number {
  if (reply === null || reply.kind === "nil") return 5;
  let bytes: number;
  switch (reply.kind) {
    case "bulk": {
      if (reply.value === null) return 5;
      const size = Buffer.byteLength(reply.value);
      if (size > DEFAULT_RESP_LIMITS.maxBulkBytes)
        invalid("RESP bulk length exceeds limit");
      bytes = size + String(size).length + 5;
      break;
    }
    case "array":
      if (
        depth >= DEFAULT_RESP_LIMITS.maxDepth ||
        reply.items.length > DEFAULT_RESP_LIMITS.maxArguments
      )
        invalid("RESP array/depth exceeds limit");
      bytes = String(reply.items.length).length + 3;
      for (const item of reply.items) {
        bytes += replyByteLength(item, depth + 1);
        if (bytes > DEFAULT_RESP_LIMITS.maxFrameBytes)
          invalid("RESP frame exceeds limit");
      }
      break;
    case "int":
      if (!Number.isSafeInteger(reply.value))
        invalid("RESP integers must be safe integers");
      bytes = String(reply.value).length + 3;
      break;
    case "simple":
      if (/[\r\n]/.test(reply.value))
        invalid("RESP simple strings cannot contain CR/LF");
      bytes = Buffer.byteLength(reply.value) + 3;
      break;
    case "error":
      bytes = Buffer.byteLength(reply.message) + 3;
      break;
  }
  if (bytes > DEFAULT_RESP_LIMITS.maxFrameBytes)
    invalid("RESP frame exceeds limit");
  return bytes;
}

export function encodeSimple(value: string): Buffer {
  replyByteLength({ kind: "simple", value });
  return Buffer.from(`+${value}\r\n`);
}
export function encodeError(message: string): Buffer {
  return Buffer.from(`-${String(message).replace(/[\r\n]/g, " ")}\r\n`);
}
export function encodeInt(value: number): Buffer {
  if (!Number.isSafeInteger(value))
    invalid("RESP integers must be safe integers");
  return Buffer.from(`:${value}\r\n`);
}
export function encodeBulk(value: string | null | undefined): Buffer {
  if (value == null) return Buffer.from("$-1\r\n");
  const body = Buffer.from(value);
  return Buffer.concat([
    Buffer.from(`$${body.length}\r\n`),
    body,
    Buffer.from("\r\n"),
  ]);
}
function encoded(reply: RespReply | null): Buffer {
  if (reply === null) return encodeBulk(null);
  switch (reply.kind) {
    case "simple":
      return encodeSimple(reply.value);
    case "error":
      return encodeError(reply.message);
    case "int":
      return encodeInt(reply.value);
    case "bulk":
      return encodeBulk(reply.value);
    case "nil":
      return Buffer.from("*-1\r\n");
    case "array":
      return Buffer.concat([
        Buffer.from(`*${reply.items.length}\r\n`),
        ...reply.items.map(encoded),
      ]);
  }
}
export function encodeReply(reply: RespReply | null): Buffer {
  replyByteLength(reply);
  return encoded(reply);
}
export function encodeArray(items: Array<RespReply | null>): Buffer {
  return encodeReply({ kind: "array", items });
}
export function commandByteLength(args: Array<string | number>): number {
  return replyByteLength({
    kind: "array",
    items: args.map((value) => ({ kind: "bulk", value: String(value) })),
  });
}
export function encodeCommand(args: Array<string | number>): Buffer {
  return encodeArray(
    args.map((value) => ({ kind: "bulk", value: String(value) })),
  );
}

/** Resumable parsing retains partial arrays and fills each bulk payload once. */
export class RespDecoder extends RespReader {
  constructor(options: RespDecoderOptions = {}) {
    const limits: Required<RespDecoderOptions> = { ...DEFAULT_RESP_LIMITS };
    for (const key of Object.keys(limits) as Array<keyof typeof limits>) {
      const value = options[key] ?? limits[key];
      if (!Number.isSafeInteger(value) || value < 1)
        invalid(`${key} must be a safe integer >= 1`);
      limits[key] = value;
    }
    super(limits);
  }
}

export function requestArgv(request: RespReply): string[] {
  if (request.kind !== "array")
    return invalid("protocol error: expected an array command");
  const argv: string[] = [];
  for (const item of request.items) {
    if (!item || item.kind !== "bulk" || item.value === null)
      return invalid("protocol error: command args must be bulk strings");
    argv.push(item.value);
  }
  if (!argv.length) invalid("protocol error: empty command");
  return argv;
}
