import { Value } from "./types";
import { DatabaseError } from "./errors";

export const MAX_JSON_DEPTH = 128;
export const MAX_JSON_NODES = 1_000_000;

/** Owned ordinary JSON objects; own data properties preserve names such as __proto__. */
export function cloneJsonValue(
  value: unknown,
  what = "value",
  maxBytes = 64 * 1024 * 1024,
): Value {
  const ancestors = new Set<object>();
  let nodes = 0;
  let bytes = 0;
  const charge = (count: number): void => {
    bytes += count;
    if (bytes > maxBytes)
      throw new DatabaseError(
        `${what} exceeds JSON byte limit (${maxBytes})`,
        "LIMIT_EXCEEDED",
      );
  };
  const stringBytes = (text: string): void => {
    if (text.length > maxBytes - bytes - 2) charge(maxBytes + 1);
    charge(2);
    for (let i = 0; i < text.length; i++) {
      const c = text.charCodeAt(i);
      if (c === 34 || c === 92 || [8, 9, 10, 12, 13].includes(c)) charge(2);
      else if (c < 32) charge(6);
      else if (c < 128) charge(1);
      else if (c < 2048) charge(2);
      else if (
        c >= 0xd800 &&
        c <= 0xdbff &&
        text.charCodeAt(i + 1) >= 0xdc00 &&
        text.charCodeAt(i + 1) <= 0xdfff
      ) {
        charge(4);
        i++;
      } else if (c >= 0xd800 && c <= 0xdfff) charge(6);
      else charge(3);
    }
  };
  const clone = (current: unknown, path: string, depth: number): Value => {
    const fail = (reason: string): never => {
      throw new DatabaseError(`${what} ${reason} at ${path}`, "INVALID_VALUE");
    };
    if (++nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH)
      fail("exceeds JSON depth/node limits");
    if (typeof current === "string") {
      stringBytes(current);
      return current;
    }
    if (current === null || typeof current === "boolean") {
      charge(current === null ? 4 : current ? 4 : 5);
      return current;
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current))
        fail("must contain only finite JSON numbers");
      charge(String(Object.is(current, -0) ? 0 : current).length);
      return Object.is(current, -0) ? 0 : current;
    }
    if (current === undefined) fail("cannot contain undefined (use null)");
    if (typeof current !== "object") fail("must contain only JSON values");
    const object = current as object;
    if (ancestors.has(object)) fail("cannot contain circular references");
    const array = Array.isArray(object);
    const prototype = Object.getPrototypeOf(object);
    if (
      array
        ? prototype !== Array.prototype
        : prototype !== Object.prototype && prototype !== null
    ) {
      fail("must contain only plain JSON objects and arrays");
    }
    if (
      Object.getOwnPropertySymbols(object).some(
        (key) => Object.getOwnPropertyDescriptor(object, key)?.enumerable,
      )
    ) {
      fail("cannot contain enumerable symbol keys");
    }
    const serializer = Object.getOwnPropertyDescriptor(object, "toJSON");
    if (
      serializer &&
      (!("value" in serializer) || typeof serializer.value === "function")
    )
      fail("cannot contain custom serialization hooks");
    if (
      array &&
      Object.keys(object).some(
        (key) =>
          !/^(0|[1-9][0-9]*)$/.test(key) ||
          Number(key) >= (object as Value[]).length,
      )
    )
      fail("cannot contain extra array properties");
    ancestors.add(object);
    try {
      const out: Value[] | Record<string, Value> = array ? [] : {};
      if (array && (object as Value[]).length > MAX_JSON_NODES - nodes)
        fail("exceeds JSON node limits");
      const keys = array
        ? Array.from({ length: (object as Value[]).length }, (_, i) =>
            String(i),
          )
        : Object.keys(object);
      if (keys.length > MAX_JSON_NODES - nodes)
        fail("exceeds JSON node limits");
      charge(2 + Math.max(0, keys.length - 1));
      for (const key of keys) {
        if (!array) {
          stringBytes(key);
          charge(1);
        }
        const descriptor = Object.getOwnPropertyDescriptor(object, key);
        if (!descriptor) fail("cannot contain sparse arrays");
        if (!descriptor || !("value" in descriptor))
          fail("cannot contain accessors");
        Object.defineProperty(out, key, {
          value: clone(descriptor!.value, `${path}.${key}`, depth + 1),
          enumerable: true,
          writable: true,
          configurable: true,
        });
      }
      return out;
    } finally {
      ancestors.delete(object);
    }
  };
  return clone(value, "$", 0);
}

export function stringifyJsonValue(value: unknown): string {
  return JSON.stringify(cloneJsonValue(value, "value", 4 * 1024 * 1024));
}
