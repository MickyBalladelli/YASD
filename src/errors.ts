/** Stable application codes; messages are diagnostic and may change. */
export const DATABASE_ERROR_CODES = Object.freeze({
  UNKNOWN_STATEMENT: 'UNKNOWN_STATEMENT', TABLE_EXISTS: 'TABLE_EXISTS', TABLE_NOT_FOUND: 'TABLE_NOT_FOUND',
  COLUMN_NOT_FOUND: 'COLUMN_NOT_FOUND', PRIMARY_KEY_CONSTRAINT: 'PRIMARY_KEY_CONSTRAINT',
  NOT_NULL_CONSTRAINT: 'NOT_NULL_CONSTRAINT', TYPE_CONSTRAINT: 'TYPE_CONSTRAINT',
  DUPLICATE_COLUMN: 'DUPLICATE_COLUMN', ROW_ARITY: 'ROW_ARITY', PARSE_ERROR: 'PARSE_ERROR',
  INVALID_VALUE: 'INVALID_VALUE', INVALID_CONFIG: 'INVALID_CONFIG', PROTOCOL_ERROR: 'PROTOCOL_ERROR',
  LIMIT_EXCEEDED: 'LIMIT_EXCEEDED', CONNECTION_CLOSED: 'CONNECTION_CLOSED', TIMEOUT: 'TIMEOUT',
  TRANSACTION_ERROR: 'TRANSACTION_ERROR', PERSISTENCE_ERROR: 'PERSISTENCE_ERROR',
  AUTH_ERROR: 'AUTH_ERROR', COMMAND_ERROR: 'COMMAND_ERROR',
} as const);
export type DatabaseErrorCode = keyof typeof DATABASE_ERROR_CODES;

export class DatabaseError extends Error {
  readonly code: DatabaseErrorCode;
  constructor(message: string, code: DatabaseErrorCode, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'DatabaseError';
    this.code = code;
  }
}

export function wireError(error: unknown): string {
  const code = error instanceof DatabaseError ? error.code : 'COMMAND_ERROR';
  const message = error instanceof Error ? error.message : String(error);
  return `ERR [${code}] ${message.replace(/[\r\n]/g, ' ')}`;
}

export function errorFromWire(message: string): DatabaseError {
  const match = /^ERR \[([A-Z_]+)\] (.*)$/.exec(message);
  if (match && Object.prototype.hasOwnProperty.call(DATABASE_ERROR_CODES, match[1])) {
    return new DatabaseError(match[2], match[1] as DatabaseErrorCode);
  }
  return new DatabaseError(message.replace(/^ERR\s*/, ''), message.startsWith('NOAUTH') ? 'AUTH_ERROR' : 'COMMAND_ERROR');
}
