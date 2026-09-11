import { Value } from './types';
import { DatabaseError } from './errors';

export const MAX_JSON_DEPTH = 128;
export const MAX_JSON_NODES = 1_000_000;

/** Owned ordinary JSON objects; own data properties preserve names such as __proto__. */
export function cloneJsonValue(value: unknown, what = 'value'): Value {
  const ancestors = new Set<object>();
  let nodes = 0;
  const clone = (current: unknown, path: string, depth: number): Value => {
    const fail = (reason: string): never => { throw new DatabaseError(`${what} ${reason} at ${path}`, 'INVALID_VALUE'); };
    if (++nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH) fail('exceeds JSON depth/node limits');
    if (current === null || typeof current === 'string' || typeof current === 'boolean') return current;
    if (typeof current === 'number') {
      if (!Number.isFinite(current)) fail('must contain only finite JSON numbers');
      return Object.is(current, -0) ? 0 : current;
    }
    if (current === undefined) fail('cannot contain undefined (use null)');
    if (typeof current !== 'object') fail('must contain only JSON values');
    const object = current as object;
    if (ancestors.has(object)) fail('cannot contain circular references');
    const array = Array.isArray(object);
    const prototype = Object.getPrototypeOf(object);
    if (array ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null) {
      fail('must contain only plain JSON objects and arrays');
    }
    if (Object.getOwnPropertySymbols(object).some(key => Object.getOwnPropertyDescriptor(object, key)?.enumerable)) {
      fail('cannot contain enumerable symbol keys');
    }
    const serializer = Object.getOwnPropertyDescriptor(object, 'toJSON');
    if (serializer && (!('value' in serializer) || typeof serializer.value === 'function')) fail('cannot contain custom serialization hooks');
    if (array && Object.keys(object).some(key => !/^(0|[1-9][0-9]*)$/.test(key))) fail('cannot contain extra array properties');
    ancestors.add(object);
    try {
      const out: Value[] | Record<string, Value> = array ? [] : {};
      if (array && (object as Value[]).length > MAX_JSON_NODES - nodes) fail('exceeds JSON node limits');
      const keys = array ? Array.from({ length: (object as Value[]).length }, (_, i) => String(i)) : Object.keys(object);
      if (keys.length > MAX_JSON_NODES - nodes) fail('exceeds JSON node limits');
      for (const key of keys) {
        const descriptor = Object.getOwnPropertyDescriptor(object, key);
        if (!descriptor) fail('cannot contain sparse arrays');
        if (!descriptor || !('value' in descriptor)) fail('cannot contain accessors');
        Object.defineProperty(out, key, {
          value: clone(descriptor!.value, `${path}.${key}`, depth + 1),
          enumerable: true, writable: true, configurable: true,
        });
      }
      return out;
    } finally {
      ancestors.delete(object);
    }
  };
  return clone(value, '$', 0);
}

export function stringifyJsonValue(value: unknown): string {
  return JSON.stringify(cloneJsonValue(value));
}
