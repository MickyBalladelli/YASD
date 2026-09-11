// Types for the YASD database

// Primitive scalar values (indexable).
export type Primitive = string | number | boolean | null;

// Recursive JSON value: Echo feeds are objects/arrays, so the cache
// and table rows must accept structured values, not just scalars.
export interface JsonObject {
  [key: string]: JsonValue;
}
export type JsonArray = JsonValue[];
export type JsonValue = Primitive | JsonArray | JsonObject;

// General value type (scalars + structured JSON).
export type Value = JsonValue;

export interface Row {
  [key: string]: Value;
}

export interface TableSchema {
  name: string;
  columns: ColumnDefinition[];
  primaryKey?: string;
}

export interface ColumnDefinition {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'any';
  nullable?: boolean;
  default?: Value;
}

export interface TableData {
  schema: TableSchema;
  rows: Row[];
  // Indexes only cover Primitive values; objects/arrays are not indexed
  // (reference-equality Map keys would be useless for lookups). Sets make
  // row membership removal O(1); row positions are rebuilt after DELETE.
  indexes: { [columnName: string]: Map<Primitive, Set<number>> };
}

export interface Database {
  tables: Map<string, TableData>;
}

// SQL AST types
export type SqlStatement = 
  | CreateTableStatement 
  | InsertStatement 
  | SelectStatement 
  | UpdateStatement 
  | DeleteStatement 
  | DropTableStatement;

export interface CreateTableStatement {
  type: 'create_table';
  tableName: string;
  columns: ColumnDefinition[];
  primaryKey?: string;
}

export interface InsertStatement {
  type: 'insert';
  tableName: string;
  columns: string[];
  values: Value[][];
}

export interface SelectStatement {
  type: 'select';
  columns: string[] | '*',
  tableName: string;
  where?: WhereClause;
  orderBy?: OrderByClause;
  limit?: number;
  offset?: number;
}

export interface UpdateStatement {
  type: 'update';
  tableName: string;
  set: { column: string; value: Value }[];
  where?: WhereClause;
}

export interface DeleteStatement {
  type: 'delete';
  tableName: string;
  where?: WhereClause;
}

export interface DropTableStatement {
  type: 'drop_table';
  tableName: string;
}

export type WhereClause = AndClause | OrClause | ComparisonClause | IsNullClause | NotClause;

export interface AndClause {
  type: 'and';
  left: WhereClause;
  right: WhereClause;
}

export interface OrClause {
  type: 'or';
  left: WhereClause;
  right: WhereClause;
}

export interface NotClause {
  type: 'not';
  clause: WhereClause;
}

export interface IsNullClause {
  type: 'is_null' | 'is_not_null';
  expression: Expression;
}

export type Expression = LiteralExpression | ColumnReferenceExpression;

export interface LiteralExpression {
  type: 'literal';
  value: Value;
}

export interface ColumnReferenceExpression {
  type: 'column_ref';
  name: string;
}

export interface ComparisonClause {
  type: 'comparison';
  left: Expression;
  operator: '=' | '!=' | '>' | '>=' | '<' | '<=' | 'like' | 'in' | 'between';
  right: Expression | Expression[];
}

export interface OrderByClause {
  column: string;
  direction: 'asc' | 'desc';
}

export interface QueryResult {
  columns: string[];
  rows: Row[];
  affectedRows?: number;
}

export interface DatabaseError extends Error {
  code: string;
  message: string;
}
