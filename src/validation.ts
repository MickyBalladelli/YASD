const DECIMAL_NUMBER = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const DECIMAL_INTEGER = /^[+-]?\d+$/;

export function validateFiniteNumber(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`${name} must be a finite number, got ${String(value)}`);
  }
  return value;
}

export function validateNonNegativeNumber(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a finite number >= 0, got ${String(value)}`);
  }
  return value;
}

export function validateSafeInteger(value: unknown, name: string, min?: number): number {
  if (!Number.isSafeInteger(value) || (min !== undefined && (value as number) < min)) {
    const bound = min === undefined ? 'a safe integer' : `a safe integer >= ${min}`;
    throw new Error(`${name} must be ${bound}, got ${String(value)}`);
  }
  return value as number;
}

export function validatePositiveSafeInteger(value: unknown, name: string): number {
  return validateSafeInteger(value, name, 1);
}

export function validateNonNegativeSafeInteger(value: unknown, name: string): number {
  return validateSafeInteger(value, name, 0);
}

export function parseStrictNumber(raw: string, name: string): number {
  const value = raw.trim();
  if (value.length === 0 || !DECIMAL_NUMBER.test(value)) {
    throw new Error(`${name} must be a finite number, got ${raw}`);
  }
  return validateFiniteNumber(Number(value), name);
}

export function parseStrictNonNegativeNumber(raw: string, name: string): number {
  const value = raw.trim();
  if (value.length === 0 || !DECIMAL_NUMBER.test(value)) {
    throw new Error(`${name} must be a finite number >= 0, got ${raw}`);
  }
  try {
    return validateNonNegativeNumber(Number(value), name);
  } catch {
    throw new Error(`${name} must be a finite number >= 0, got ${raw}`);
  }
}

export function parseStrictInteger(raw: string, name: string, min?: number): number {
  const value = raw.trim();
  if (value.length === 0 || !DECIMAL_INTEGER.test(value)) {
    throw new Error(`${name} must be a safe integer${min === undefined ? '' : ` >= ${min}`}, got ${raw}`);
  }
  return validateSafeInteger(Number(value), name, min);
}

export function validatePort(value: unknown, name = 'port'): number {
  const port = validateSafeInteger(value, name, 0);
  if (port > 65_535) {
    throw new Error(`${name} must be between 0 and 65535, got ${String(value)}`);
  }
  return port;
}

export function parsePort(raw: string, name = 'port'): number {
  const port = parseStrictInteger(raw, name, 0);
  return validatePort(port, name);
}

export function validateHost(value: unknown, name = 'host'): string {
  if (typeof value !== 'string' || value.trim().length === 0 || /[\u0000-\u0020]/.test(value)) {
    throw new Error(`${name} must be a non-empty host name`);
  }
  return value;
}
