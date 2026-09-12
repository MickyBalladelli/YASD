#!/usr/bin/env node
// Deterministic property/fuzz tests for the SQL parser and RESP decoder.
// Run: node test/fuzz.test.js (needs the built dist).

let mod;
try {
  mod = require("../dist/index.js");
  console.log("Using compiled version from dist/index.js");
} catch {
  console.error("YASD build missing: run npm run build before running tests");
  process.exit(1);
}

const assert = require("assert");
const { parse, RespDecoder, encodeCommand, requestArgv } = mod;

function rng(seed) {
  let state = seed >>> 0;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
}

function int(random, min, max) {
  return min + (random() % (max - min + 1));
}

function randomText(random, maxLength = 40) {
  const alphabet = "abcXYZ 012-'\"\\é🚀";
  let text = "";
  for (let i = 0; i < int(random, 0, maxLength); i++) {
    text += alphabet[int(random, 0, alphabet.length - 1)];
  }
  return text;
}

function sqlQuote(value) {
  return `'${sqlEscape(value)}'`;
}

function sqlEscape(value) {
  return value.replaceAll("\\", "\\\\").replaceAll("'", "''");
}

function assertError(fn, pattern) {
  assert.throws(fn, (error) => {
    assert.ok(error instanceof Error);
    return pattern === undefined || pattern.test(error.message);
  });
}

function parserProperties() {
  const random = rng(0x51a7c0de);
  const numbers = [
    "0",
    "-0",
    "1",
    "-42",
    "3.14",
    ".5",
    "10.",
    "1e3",
    "-2.5e-2",
  ];

  for (let i = 0; i < 400; i++) {
    const text = randomText(random);
    const number = numbers[int(random, 0, numbers.length - 1)];
    const boolean = i % 2 === 0;
    const statement = parse(
      `INSERT INTO fuzz VALUES (${number}, ${sqlQuote(text)}, ${boolean ? "TRUE" : "false"})`,
    );
    assert.deepStrictEqual(statement.values[0], [
      Number(number),
      text,
      boolean,
    ]);
  }

  for (let i = 0; i < 120; i++) {
    const text = sqlEscape(randomText(random));
    assertError(
      () => parse(`INSERT INTO fuzz VALUES ('${text}`),
      /Unterminated quoted string/,
    );
    assertError(
      () => parse(`INSERT INTO fuzz VALUES ('${text}\\q')`),
      /Unsupported escape/,
    );
  }

  const malformedNumbers = ["10oops", "1.2.3", "1e+", "1e--2", "-7bad", ".5.2"];
  for (const number of malformedNumbers) {
    assertError(
      () => parse(`INSERT INTO fuzz VALUES (${number})`),
      /Malformed numeric/,
    );
  }

  const base = "SELECT * FROM fuzz";
  const suffixes = [
    " garbage",
    " SELECT * FROM fuzz",
    "; SELECT * FROM fuzz",
    ";;",
  ];
  for (let i = 0; i < 100; i++) {
    assertError(
      () => parse(base + suffixes[int(random, 0, suffixes.length - 1)]),
      /trailing token/,
    );
  }

  for (let i = 0; i < 250; i++) {
    const garbage =
      randomText(random, 120) + String.fromCharCode(int(random, 0, 31));
    try {
      parse(garbage);
    } catch (error) {
      assert.ok(error instanceof Error);
    }
  }

  const large = "x".repeat(256 * 1024);
  const largeStatement = parse(`INSERT INTO fuzz VALUES (${sqlQuote(large)})`);
  assert.strictEqual(largeStatement.values[0][0], large);
}

function decodeWithRandomChunks(frame, random, decoder = new RespDecoder()) {
  const replies = [];
  let offset = 0;
  while (offset < frame.length) {
    const end = Math.min(frame.length, offset + int(random, 1, 19));
    replies.push(...decoder.push(frame.subarray(offset, end)));
    offset = end;
  }
  return replies;
}

function nestedArray(depth) {
  let frame = Buffer.from("$1\r\nx\r\n");
  for (let i = 0; i < depth; i++) {
    frame = Buffer.concat([Buffer.from("*1\r\n"), frame]);
  }
  return frame;
}

function respProperties() {
  const random = rng(0xdec0de42);

  for (let i = 0; i < 300; i++) {
    const args = [
      "SET",
      `fuzz:${i}`,
      JSON.stringify({
        text: randomText(random),
        value: int(random, -1000, 1000),
      }),
      "PX",
      String(int(random, 0, 60000)),
    ];
    const frame = encodeCommand(args);
    const replies = decodeWithRandomChunks(frame, random);
    assert.strictEqual(replies.length, 1);
    assert.deepStrictEqual(requestArgv(replies[0]), args);
  }

  const batch = Buffer.concat(
    Array.from({ length: 40 }, (_, i) => encodeCommand(["PING", `chunk-${i}`])),
  );
  const batchReplies = decodeWithRandomChunks(batch, random);
  assert.deepStrictEqual(
    batchReplies.map((reply) => requestArgv(reply)),
    Array.from({ length: 40 }, (_, i) => ["PING", `chunk-${i}`]),
  );

  const malformedLengths = [
    "$1oops\r\nx\r\n",
    "$1.0\r\nx\r\n",
    "$+1\r\nx\r\n",
    "$-2\r\n",
    "$999999999999999999999999\r\n",
    "*1oops\r\n",
    "*1.0\r\n",
    "*+1\r\n",
    "*-2\r\n",
    ":1oops\r\n",
  ];
  for (const frame of malformedLengths) {
    assertError(() => new RespDecoder().push(frame), /malformed RESP/);
  }

  for (let i = 0; i < 100; i++) {
    const maxDepth = int(random, 1, 16);
    const tooDeep = nestedArray(maxDepth + int(random, 1, 20));
    assertError(
      () => new RespDecoder({ maxDepth }).push(tooDeep),
      /nesting exceeds/,
    );

    const validDepth = int(random, 0, maxDepth - 1);
    const valid = new RespDecoder({ maxDepth });
    assert.strictEqual(valid.push(nestedArray(validDepth)).length, 1);
  }

  const invalidUtf8 = [
    [0xff],
    [0xc0, 0xaf],
    [0xed, 0xa0, 0x80],
    [0xf4, 0x90, 0x80, 0x80],
  ];
  for (const bytes of invalidUtf8) {
    const frame = Buffer.concat([
      Buffer.from(`$${bytes.length}\r\n`),
      Buffer.from(bytes),
      Buffer.from("\r\n"),
    ]);
    assertError(() => new RespDecoder().push(frame), /invalid RESP UTF-8/);
  }

  const largePayload = encodeCommand(["SET", "large", "x".repeat(256 * 1024)]);
  assertError(
    () => new RespDecoder({ maxFrameBytes: 64 * 1024 }).push(largePayload),
    /frame exceeds/,
  );
  assertError(
    () =>
      new RespDecoder({ maxBufferedBytes: 1024 }).push(
        Buffer.alloc(2048, 0x2a),
      ),
    /buffered bytes exceed/,
  );

  for (let i = 0; i < 250; i++) {
    const bytes = Buffer.alloc(int(random, 1, 160));
    for (let j = 0; j < bytes.length; j++) bytes[j] = int(random, 0, 255);
    try {
      new RespDecoder({ maxBufferedBytes: 512 }).push(bytes);
    } catch (error) {
      assert.ok(error instanceof Error);
    }
  }
}

let passed = 0;
let failed = 0;
for (const [name, fn] of [
  ["SQL parser properties and malformed inputs", parserProperties],
  ["RESP decoder properties and malformed inputs", respProperties],
]) {
  try {
    fn();
    console.log(`✓ ${name}`);
    passed++;
  } catch (error) {
    console.error(`✗ ${name}`);
    console.error(`  Error: ${error.message}`);
    failed++;
  }
}

console.log(`Fuzz tests completed: ${passed + failed}`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
if (failed > 0) process.exit(1);
