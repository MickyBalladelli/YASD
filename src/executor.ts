// Query Executor for YASD
// Executes parsed SQL statements against the in-memory database

import {
  Database,
  TableData,
  Row,
  Value,
  Primitive,
  SqlStatement,
  CreateTableStatement,
  InsertStatement,
  SelectStatement,
  UpdateStatement,
  DeleteStatement,
  DropTableStatement,
  WhereClause,
  QueryResult,
  ComparisonClause,
  AndClause,
  OrClause,
  NotClause,
  Expression,
} from './types';
import { parse } from './parser';
import { performance } from 'perf_hooks';
import { SlowLog, checkSlowThreshold } from './metrics';

class DatabaseError extends Error {
  constructor(message: string, public code: string) {
    super(message);
    this.name = 'DatabaseError';
  }
}

export interface SlowQueryEntry {
  sql: string;
  durationMs: number;
  at: number;
}

export interface QueryPlan {
  statement: string;
  table?: string;
  columns?: string[] | '*';
  /** How the WHERE filter is served. */
  strategy: 'index-scan' | 'full-scan' | 'n/a';
  /** Column(s) served from an index, when strategy is index-scan. */
  indexColumns?: string[];
  hasOrderBy: boolean;
  /** ORDER BY path detail (column + direction) when present. */
  orderBy?: { column: string; direction: 'asc' | 'desc' };
  limit?: number;
  offset?: number;
  /** Live row count of the table at plan time. */
  tableRows?: number;
}

export interface QueryProfile extends QueryPlan {
  durationMs: number;
  rowsReturned: number;
  affectedRows?: number;
}

export class Executor {
  private db: Database;
  private slow = new SlowLog();

  constructor() {
    this.db = { tables: new Map() };
  }

  /** Log queries slower than this (ms). 0 disables. */
  setSlowQueryThreshold(ms: number): void {
    this.slow.setThreshold(checkSlowThreshold(ms, 'slow query threshold'));
  }

  getSlowQueryThreshold(): number {
    return this.slow.threshold;
  }

  /** Newest-first ring of slow queries (capped). */
  getSlowLog(): SlowQueryEntry[] {
    return this.slow.list().map(e => ({ sql: e.name, durationMs: e.durationMs, at: e.at }));
  }

  clearSlowLog(): void {
    this.slow.clear();
  }

  execute(sql: string): QueryResult {
    // Single parser path: src/parser.ts is the only SQL frontend.
    // (The old parseSimple duplicate lived here; it was removed so behavior
    // is consistent no matter the entry point.)
    const text = sql.trim();

    const started = performance.now();
    try {
      const statement = parse(text);
      return this.executeStatement(statement);
    } finally {
      this.slow.record(text, performance.now() - started);
    }
  }

  executeStatement(statement: SqlStatement): QueryResult {
    switch (statement.type) {
      case 'create_table':
        return this.executeCreateTable(statement);
      case 'insert':
        return this.executeInsert(statement);
      case 'select':
        return this.executeSelect(statement);
      case 'update':
        return this.executeUpdate(statement);
      case 'delete':
        return this.executeDelete(statement);
      case 'drop_table':
        return this.executeDropTable(statement);
      default:
        throw new DatabaseError(`Unknown statement type: ${(statement as any).type}`, 'UNKNOWN_STATEMENT');
    }
  }

  // ---- index helpers ----
  // Tables maintain a per-column Map<Primitive, rowPositions[]> on write.
  // These helpers let reads use them instead of always full-scanning.

  private isIndexableValue(v: unknown): v is Primitive {
    return v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean';
  }

  private isColumn(table: TableData, name: unknown): name is string {
    return typeof name === 'string' && table.schema.columns.some(c => c.name === name);
  }

  private indexGet(table: TableData, column: string, value: Primitive): number[] | undefined {
    const index = table.indexes[column];
    if (!index) return undefined;
    const rows = index.get(value);
    return rows ? [...rows] : [];
  }

  /**
   * Plan an index lookup for a WHERE clause.
   * Returns candidate row positions, or undefined when the predicate is not
   * indexable (caller falls back to a full scan). Supports `col = const`,
   * `const = col`, `col IN (...)`, and ANDs of indexable predicates.
   * Results are always re-checked with evaluateWhere, so a stale index entry
   * can only cost time, never correctness.
   */
  private planIndexLookup(table: TableData, where: WhereClause): number[] | undefined {
    switch (where.type) {
      case 'comparison': {
        const comp = where as ComparisonClause;
        if (comp.operator === '=') {
          if (
            comp.left.type === 'column_ref' &&
            this.isColumn(table, comp.left.name) &&
            !Array.isArray(comp.right) &&
            comp.right.type === 'literal' &&
            this.isIndexableValue(comp.right.value)
          ) {
            return this.indexGet(table, comp.left.name, comp.right.value);
          }
          if (
            comp.left.type === 'literal' &&
            this.isIndexableValue(comp.left.value) &&
            !Array.isArray(comp.right) &&
            comp.right.type === 'column_ref' &&
            this.isColumn(table, comp.right.name)
          ) {
            return this.indexGet(table, comp.right.name, comp.left.value);
          }
          return undefined;
        }
        if (comp.operator === 'in') {
          if (
            comp.left.type === 'column_ref' &&
            this.isColumn(table, comp.left.name) &&
            Array.isArray(comp.right)
          ) {
            const out = new Set<number>();
            for (const v of comp.right) {
              if (v.type !== 'literal' || !this.isIndexableValue(v.value)) return undefined;
              const rows = this.indexGet(table, comp.left.name, v.value);
              if (rows === undefined) return undefined;
              for (const i of rows) out.add(i);
            }
            return [...out].sort((a, b) => a - b);
          }
          return undefined;
        }
        return undefined;
      }
      case 'and': {
        const and = where as AndClause;
        const left = this.planIndexLookup(table, and.left);
        const right = this.planIndexLookup(table, and.right);
        if (left === undefined || right === undefined) return undefined;
        const rightSet = new Set(right);
        return left.filter(i => rightSet.has(i));
      }
      default:
        return undefined;
    }
  }

  /** Rebuild every column index in a single O(rows x cols) pass. */
  private rebuildIndexes(table: TableData): void {
    for (const colDef of table.schema.columns) {
      const index = table.indexes[colDef.name];
      if (!index) continue;
      index.clear();
      for (let i = 0; i < table.rows.length; i++) {
        const value = table.rows[i][colDef.name];
        if (!this.isIndexableValue(value)) continue; // objects/arrays aren't indexed
        let list = index.get(value);
        if (!list) {
          list = [];
          index.set(value, list);
        }
        list.push(i);
      }
    }
  }

  // Execute methods

  private executeCreateTable(statement: CreateTableStatement): QueryResult {
    const { tableName, columns, primaryKey } = statement;

    if (this.db.tables.has(tableName)) {
      throw new DatabaseError(`Table '${tableName}' already exists`, 'TABLE_EXISTS');
    }

    const tableData: TableData = {
      schema: {
        name: tableName,
        columns,
        primaryKey
      },
      rows: [],
      indexes: {}
    };

    // Create indexes for primary key
    if (primaryKey) {
      tableData.indexes[primaryKey] = new Map();
    }

    // Create indexes for all columns (for simple queries)
    for (const col of columns) {
      tableData.indexes[col.name] = new Map();
    }

    this.db.tables.set(tableName, tableData);

    return {
      columns: [],
      rows: [],
      affectedRows: 0
    };
  }

  private executeInsert(statement: InsertStatement): QueryResult {
    const { tableName, columns, values } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(`Table '${tableName}' not found`, 'TABLE_NOT_FOUND');
    }

    const colDefs = table.schema.columns;
    let affectedRows = 0;

    for (const rowValues of values) {
      const row: Row = {};

      // Fill in values for specified columns or all columns in order
      if (columns.length > 0) {
        // Values for specified columns
        for (let i = 0; i < columns.length; i++) {
          const colName = columns[i];
          const colDef = colDefs.find(c => c.name === colName);

          if (!colDef) {
            throw new DatabaseError(`Column '${colName}' not found in table '${tableName}'`, 'COLUMN_NOT_FOUND');
          }

          let value = rowValues[i];

          // Convert value based on column type
          if (colDef.type === 'number' && typeof value === 'string') {
            value = Number(value);
          } else if (colDef.type === 'boolean' && typeof value === 'string') {
            value = value.toLowerCase() === 'true';
          }

          row[colName] = value;
        }
      } else {
        // Values for all columns in table order
        for (let i = 0; i < colDefs.length && i < rowValues.length; i++) {
          const colDef = colDefs[i];
          let value = rowValues[i];

          // Convert value based on column type
          if (colDef.type === 'number' && typeof value === 'string') {
            value = Number(value);
          } else if (colDef.type === 'boolean' && typeof value === 'string') {
            value = value.toLowerCase() === 'true';
          }

          row[colDef.name] = value;
        }
      }

      // Fill in default values for unspecified columns
      for (const colDef of colDefs) {
        if (!row.hasOwnProperty(colDef.name)) {
          if (colDef.default !== undefined) {
            row[colDef.name] = colDef.default;
          } else if (!colDef.nullable) {
            throw new DatabaseError(`Column '${colDef.name}' cannot be null`, 'NOT_NULL_CONSTRAINT');
          } else {
            row[colDef.name] = null;
          }
        }
      }

      // Add to table
      const rowIndex = table.rows.length;
      table.rows.push(row);

      // Update indexes (primitives only; objects/arrays are skipped)
      for (const colDef of colDefs) {
        const index = table.indexes[colDef.name];
        const value = row[colDef.name];
        if (index && this.isIndexableValue(value)) {
          if (!index.has(value)) {
            index.set(value, []);
          }
          index.get(value)!.push(rowIndex);
        }
      }

      affectedRows++;
    }

    return {
      columns: [],
      rows: [],
      affectedRows
    };
  }

  private executeSelect(statement: SelectStatement): QueryResult {
    const { columns, tableName, where, orderBy, limit, offset } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(`Table '${tableName}' not found`, 'TABLE_NOT_FOUND');
    }

    // WHERE: prefer the column index for `=` / `IN` (incl. ANDs of those);
    // anything else falls back to a full scan. Index candidates are always
    // re-checked with evaluateWhere so staleness can't affect correctness.
    let rows: Row[];
    const planned = where ? this.planIndexLookup(table, where) : undefined;
    if (planned !== undefined) {
      rows = [];
      for (const i of planned) {
        const row = table.rows[i];
        if (row && (!where || this.evaluateWhere(row, where, table))) {
          rows.push(row);
        }
      }
    } else if (where) {
      rows = table.rows.filter(row => this.evaluateWhere(row, where, table));
    } else {
      rows = [...table.rows];
    }

    // Apply ORDER BY
    if (orderBy) {
      rows.sort((a, b) => {
        const aVal = a[orderBy.column];
        const bVal = b[orderBy.column];

        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        let comparison = 0;
        if (typeof aVal === 'number' && typeof bVal === 'number') {
          comparison = aVal - bVal;
        } else {
          comparison = String(aVal).localeCompare(String(bVal));
        }

        return orderBy.direction === 'asc' ? comparison : -comparison;
      });
    }

    // Apply OFFSET and LIMIT
    if (offset !== undefined) {
      rows = rows.slice(offset);
    }
    if (limit !== undefined) {
      rows = rows.slice(0, limit);
    }

    // Select columns
    const selectedColumns = columns === '*'
      ? table.schema.columns.map(c => c.name)
      : columns;

    const resultRows = rows.map(row => {
      const resultRow: Row = {};
      for (const col of selectedColumns) {
        resultRow[col] = row[col] ?? null;
      }
      return resultRow;
    });

    return {
      columns: selectedColumns,
      rows: resultRows
    };
  }

  private executeUpdate(statement: UpdateStatement): QueryResult {
    const { tableName, set, where } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(`Table '${tableName}' not found`, 'TABLE_NOT_FOUND');
    }

    let affectedRows = 0;

    for (let i = 0; i < table.rows.length; i++) {
      const row = table.rows[i];

      // Check WHERE condition
      if (where && !this.evaluateWhere(row, where, table)) {
        continue;
      }

      // Apply SET changes
      for (const { column, value } of set) {
        const colDef = table.schema.columns.find(c => c.name === column);
        if (!colDef) {
          throw new DatabaseError(`Column '${column}' not found in table '${tableName}'`, 'COLUMN_NOT_FOUND');
        }

        let finalValue = value;
        if (colDef.type === 'number' && typeof value === 'string') {
          finalValue = Number(value);
        } else if (colDef.type === 'boolean' && typeof value === 'string') {
          finalValue = value.toLowerCase() === 'true';
        }

        // Remove old value from index
        const index = table.indexes[column];
        if (index && row[column] !== undefined && this.isIndexableValue(row[column])) {
          const oldValue = row[column] as Primitive;
          const indices = index.get(oldValue);
          if (indices) {
            const pos = indices.indexOf(i);
            if (pos > -1) {
              indices.splice(pos, 1);
              if (indices.length === 0) {
                index.delete(oldValue);
              }
            }
          }
        }

        // Update value
        row[column] = finalValue;

        // Add new value to index
        if (index && this.isIndexableValue(finalValue)) {
          if (!index.has(finalValue)) {
            index.set(finalValue, []);
          }
          index.get(finalValue)!.push(i);
        }
      }

      affectedRows++;
    }

    return {
      columns: [],
      rows: [],
      affectedRows
    };
  }

  private executeDelete(statement: DeleteStatement): QueryResult {
    const { tableName, where } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(`Table '${tableName}' not found`, 'TABLE_NOT_FOUND');
    }

    if (!where) {
      const affectedRows = table.rows.length;
      table.rows = [];
      for (const colName of Object.keys(table.indexes)) {
        table.indexes[colName] = new Map();
      }
      return { columns: [], rows: [], affectedRows };
    }

    const keep: Row[] = [];
    let affectedRows = 0;
    for (const row of table.rows) {
      if (this.evaluateWhere(row, where, table)) {
        affectedRows++;
      } else {
        keep.push(row);
      }
    }
    table.rows = keep;
    // Single-pass rebuild replaces the old O(n^2) per-deleted-row fixup loop.
    this.rebuildIndexes(table);

    return {
      columns: [],
      rows: [],
      affectedRows
    };
  }

  private executeDropTable(statement: DropTableStatement): QueryResult {
    const { tableName } = statement;

    if (!this.db.tables.has(tableName)) {
      throw new DatabaseError(`Table '${tableName}' not found`, 'TABLE_NOT_FOUND');
    }

    this.db.tables.delete(tableName);

    return {
      columns: [],
      rows: [],
      affectedRows: 0
    };
  }

  private evaluateWhere(row: Row, where: WhereClause, table: TableData): boolean {
    switch (where.type) {
      case 'and':
        return this.evaluateWhere(row, (where as AndClause).left, table) &&
               this.evaluateWhere(row, (where as AndClause).right, table);
      case 'or':
        return this.evaluateWhere(row, (where as OrClause).left, table) ||
               this.evaluateWhere(row, (where as OrClause).right, table);
      case 'not':
        return !this.evaluateWhere(row, (where as NotClause).clause, table);
      case 'comparison':
        return this.evaluateComparison(row, where as ComparisonClause, table);
      default:
        return false;
    }
  }

  private valuesEqual(a: Value, b: Value): boolean {
    if (a === b) return true;
    // Structured values (objects/arrays) compare by content.
    if (typeof a === 'object' && a !== null && typeof b === 'object' && b !== null) {
      try {
        return JSON.stringify(a) === JSON.stringify(b);
      } catch {
        return false;
      }
    }
    return false;
  }

  private evaluateExpression(row: Row, expression: Expression): Value {
    if (expression.type === 'column_ref') {
      return row[expression.name];
    }
    return expression.value;
  }

  private evaluateComparison(row: Row, comp: ComparisonClause, _table: TableData): boolean {
    const { left, operator, right } = comp;

    const leftValue = this.evaluateExpression(row, left);
    let rightValue: Value | Value[];
    if (Array.isArray(right)) {
      rightValue = right.map(expression => this.evaluateExpression(row, expression));
    } else {
      rightValue = this.evaluateExpression(row, right);
    }

    // Handle NULL comparisons
    if (leftValue === null || leftValue === undefined) {
      return operator === '!=';
    }
    if (Array.isArray(rightValue) && rightValue.length === 0) {
      return false;
    }

    switch (operator) {
      case '=':
        return !Array.isArray(rightValue) && this.valuesEqual(leftValue, rightValue);
      case '!=':
        return Array.isArray(rightValue) || !this.valuesEqual(leftValue, rightValue);
      case '>':
        return this.compareValues(leftValue, rightValue) > 0;
      case '>=':
        return this.compareValues(leftValue, rightValue) >= 0;
      case '<':
        return this.compareValues(leftValue, rightValue) < 0;
      case '<=':
        return this.compareValues(leftValue, rightValue) <= 0;
      case 'like':
        return this.likeCompare(String(leftValue), String(rightValue));
      case 'in':
        if (Array.isArray(rightValue)) {
          return rightValue.some(v => !Array.isArray(v) && this.valuesEqual(leftValue, v as Value));
        }
        return false;
      case 'between':
        if (Array.isArray(rightValue) && rightValue.length === 2) {
          const [low, high] = rightValue;
          return this.compareValues(leftValue, low) >= 0 &&
                 this.compareValues(leftValue, high) <= 0;
        }
        return false;
      default:
        return false;
    }
  }

  private compareValues(a: Value, b: Value | Value[]): number {
    if (Array.isArray(b)) {
      b = b[0];
    }

    if (a === null || a === undefined) return -1;
    if (b === null || b === undefined) return 1;

    if (typeof a === 'number' && typeof b === 'number') {
      return a - b;
    }

    return String(a).localeCompare(String(b));
  }

  private likeCompare(value: string, pattern: string): boolean {
    // Simple LIKE implementation (supports % and _)
    // Convert LIKE pattern to regex
    let regexPattern = '';
    for (let i = 0; i < pattern.length; i++) {
      const char = pattern[i];
      if (char === '%') {
        regexPattern += '.*';
      } else if (char === '_') {
        regexPattern += '.';
      } else {
        regexPattern += char.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      }
    }

    const regex = new RegExp(`^${regexPattern}$`, 'i');
    return regex.test(value);
  }

  // Public API

  query(sql: string): QueryResult {
    return this.execute(sql);
  }

  /**
   * Explain how a SELECT is served without running it: index-scan vs
   * full-scan for the WHERE filter, plus ORDER BY / LIMIT / OFFSET shape.
   * Non-SELECT statements report strategy 'n/a'.
   */
  explain(sql: string): QueryPlan {
    const text = sql.trim();
    const statement = parse(text);
    if (statement.type !== 'select') {
      return { statement: statement.type, strategy: 'n/a', hasOrderBy: false };
    }
    const table = this.db.tables.get(statement.tableName);
    const plan: QueryPlan = {
      statement: 'select',
      table: statement.tableName,
      columns: statement.columns,
      strategy: 'full-scan',
      hasOrderBy: statement.orderBy !== undefined,
      tableRows: table?.rows.length,
    };
    if (statement.orderBy !== undefined) {
      plan.orderBy = { column: statement.orderBy.column, direction: statement.orderBy.direction };
    }
    if (statement.limit !== undefined) plan.limit = statement.limit;
    if (statement.offset !== undefined) plan.offset = statement.offset;
    if (table && statement.where) {
      const cols = this.describeIndexUse(table, statement.where);
      if (cols) {
        plan.strategy = 'index-scan';
        plan.indexColumns = cols;
      }
    } else if (table && !statement.where) {
      plan.strategy = 'full-scan';
    }
    return plan;
  }

  /** Run a query and report timing + shape (plan, rows, duration). */
  profile(sql: string): QueryProfile {
    const plan = this.explain(sql);
    const started = performance.now();
    const result = this.execute(sql);
    return {
      ...plan,
      durationMs: performance.now() - started,
      rowsReturned: result.rows.length,
      ...(result.affectedRows === undefined ? {} : { affectedRows: result.affectedRows }),
    };
  }

  /**
   * Columns an index lookup would use for this WHERE (mirrors
   * planIndexLookup). Returns undefined when it falls back to a full scan.
   */
  private describeIndexUse(table: TableData, where: WhereClause): string[] | undefined {
    switch (where.type) {
      case 'comparison': {
        const comp = where as ComparisonClause;
        if (comp.operator === '=') {
          if (
            comp.left.type === 'column_ref' &&
            this.isColumn(table, comp.left.name) &&
            !Array.isArray(comp.right) &&
            comp.right.type === 'literal' &&
            this.isIndexableValue(comp.right.value)
          ) {
            return table.indexes[comp.left.name] ? [comp.left.name] : undefined;
          }
          if (
            comp.left.type === 'literal' &&
            this.isIndexableValue(comp.left.value) &&
            !Array.isArray(comp.right) &&
            comp.right.type === 'column_ref' &&
            this.isColumn(table, comp.right.name)
          ) {
            return table.indexes[comp.right.name] ? [comp.right.name] : undefined;
          }
          return undefined;
        }
        if (comp.operator === 'in') {
          if (
            comp.left.type === 'column_ref' &&
            this.isColumn(table, comp.left.name) &&
            Array.isArray(comp.right) &&
            comp.right.every(v => v.type === 'literal' && this.isIndexableValue(v.value)) &&
            table.indexes[comp.left.name]
          ) {
            return [comp.left.name];
          }
          return undefined;
        }
        return undefined;
      }
      case 'and': {
        const and = where as AndClause;
        const left = this.describeIndexUse(table, and.left);
        const right = this.describeIndexUse(table, and.right);
        if (!left || !right) return undefined;
        return Array.from(new Set([...left, ...right]));
      }
      default:
        return undefined;
    }
  }

  getTableNames(): string[] {
    return Array.from(this.db.tables.keys());
  }

  getTableSchema(tableName: string) {
    const table = this.db.tables.get(tableName);
    return table?.schema;
  }

  reset(): void {
    this.db = { tables: new Map() };
  }
}
