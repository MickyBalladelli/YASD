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
  ColumnDefinition,
  WhereClause,
  QueryResult,
  ComparisonClause,
  AndClause,
  OrClause,
  NotClause,
  IsNullClause,
  Expression,
} from "./types";
import { parse } from "./parser";
import { performance } from "perf_hooks";
import { SlowLog, checkSlowThreshold } from "./metrics";
import { structuralEqual } from "./value";

type TruthValue = boolean | null;

import { DatabaseError } from "./errors";
import { cloneJsonValue } from "./json";
import { valueIdentity, TopK, matchLike } from "./query-utils";
import { validatePositiveSafeInteger } from "./validation";

export interface SlowQueryEntry {
  sql: string;
  durationMs: number;
  at: number;
}

export interface QueryPlan {
  statement: string;
  table?: string;
  columns?: string[] | "*";
  /** How the WHERE filter is served. */
  strategy: "index-scan" | "full-scan" | "n/a";
  /** Column(s) served from an index, when strategy is index-scan. */
  indexColumns?: string[];
  hasOrderBy: boolean;
  /** ORDER BY path detail (column + direction) when present. */
  orderBy?: { column: string; direction: "asc" | "desc" };
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

export interface ExecutorOptions {
  /** Columns to index on every table. Omit to index all columns; [] disables automatic indexes. */
  indexColumns?: string[];
  maxRows?: number;
  maxBytes?: number;
  maxResultRows?: number;
  maxResultBytes?: number;
  maxTables?: number;
}

export class Executor {
  private db: Database;
  private slow = new SlowLog();
  private indexColumns?: Set<string>;
  private readonly limits: Required<Omit<ExecutorOptions, "indexColumns">>;
  private storedRows = 0;
  private storedBytes = 0;
  private examinedRows = 0;
  private statements = new Map<string, SqlStatement>();
  private statementBytes = 0;

  private prepared(sql: string): SqlStatement {
    const cached = this.statements.get(sql);
    if (cached) return cached;
    const statement = parse(sql);
    const bytes = Buffer.byteLength(sql);
    if (bytes <= 64 * 1024) {
      while (
        this.statements.size >= 128 ||
        this.statementBytes + bytes > 1024 * 1024
      ) {
        const key = this.statements.keys().next().value as string;
        this.statements.delete(key);
        this.statementBytes -= Buffer.byteLength(key);
      }
      this.statements.set(sql, statement);
      this.statementBytes += bytes;
    }
    return statement;
  }

  stats() {
    return {
      rows: this.storedRows,
      bytes: this.storedBytes,
      tables: this.db.tables.size,
      lastRowsExamined: this.examinedRows,
      preparedStatements: this.statements.size,
      limits: { ...this.limits },
    };
  }
  private checkCapacity(rows: number, bytes: number): void {
    if (rows > this.limits.maxRows || bytes > this.limits.maxBytes) {
      throw new DatabaseError("SQL storage limit exceeded", "LIMIT_EXCEEDED");
    }
  }

  constructor(options: ExecutorOptions = {}) {
    if (
      options === null ||
      typeof options !== "object" ||
      Array.isArray(options)
    ) {
      throw new Error("executor options must be an object");
    }
    if (options.indexColumns !== undefined) {
      if (!Array.isArray(options.indexColumns)) {
        throw new Error("indexColumns must be an array of column names");
      }
      const columns = new Set<string>();
      for (const column of options.indexColumns) {
        if (
          typeof column !== "string" ||
          !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(column)
        ) {
          throw new Error(`invalid index column '${String(column)}'`);
        }
        if (columns.has(column)) {
          throw new Error(`duplicate index column '${column}'`);
        }
        columns.add(column);
      }
      this.indexColumns = columns;
    }
    this.limits = {
      maxRows: validatePositiveSafeInteger(
        options.maxRows ?? 1_000_000,
        "SQL maxRows",
      ),
      maxBytes: validatePositiveSafeInteger(
        options.maxBytes ?? 256 * 1024 * 1024,
        "SQL maxBytes",
      ),
      maxResultRows: validatePositiveSafeInteger(
        options.maxResultRows ?? 100_000,
        "SQL maxResultRows",
      ),
      maxResultBytes: validatePositiveSafeInteger(
        options.maxResultBytes ?? 16 * 1024 * 1024,
        "SQL maxResultBytes",
      ),
      maxTables: validatePositiveSafeInteger(
        options.maxTables ?? 1024,
        "SQL maxTables",
      ),
    };
    this.db = { tables: new Map() };
  }

  /** Log queries slower than this (ms). 0 disables. */
  setSlowQueryThreshold(ms: number): void {
    this.slow.setThreshold(checkSlowThreshold(ms, "slow query threshold"));
  }

  getSlowQueryThreshold(): number {
    return this.slow.threshold;
  }

  /** Newest-first ring of slow queries (capped). */
  getSlowLog(): SlowQueryEntry[] {
    return this.slow
      .list()
      .map((e) => ({ sql: e.name, durationMs: e.durationMs, at: e.at }));
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
      const statement = this.prepared(text);
      return this.executeStatement(statement);
    } finally {
      this.slow.record(text, performance.now() - started);
    }
  }

  executeStatement(statement: SqlStatement): QueryResult {
    switch (statement.type) {
      case "create_table":
        return this.executeCreateTable(statement);
      case "insert":
        return this.executeInsert(statement);
      case "select":
        return this.executeSelect(statement);
      case "update":
        return this.executeUpdate(statement);
      case "delete":
        return this.executeDelete(statement);
      case "drop_table":
        return this.executeDropTable(statement);
      default:
        throw new DatabaseError(
          `Unknown statement type: ${(statement as any).type}`,
          "UNKNOWN_STATEMENT",
        );
    }
  }

  // ---- index helpers ----
  // Tables maintain a per-column Map<Primitive, Set<rowPosition>> on write.
  // These helpers let reads use them instead of always full-scanning.

  private isIndexableValue(v: unknown): v is Primitive {
    return (
      v === null ||
      typeof v === "string" ||
      typeof v === "number" ||
      typeof v === "boolean"
    );
  }

  private isColumn(table: TableData, name: unknown): name is string {
    return (
      typeof name === "string" &&
      table.schema.columns.some((c) => c.name === name)
    );
  }

  private shouldIndexColumn(column: string): boolean {
    return this.indexColumns === undefined || this.indexColumns.has(column);
  }

  private indexGet(
    table: TableData,
    column: string,
    value: Primitive,
  ): number[] | undefined {
    const index = table.indexes[column];
    if (!index) return undefined;
    const rows = index.get(value);
    return rows ? [...rows] : [];
  }

  private addIndexEntry(
    table: TableData,
    column: string,
    value: Value,
    rowIndex: number,
  ): void {
    const index = table.indexes[column];
    if (!index || !this.isIndexableValue(value)) return;
    let positions = index.get(value);
    if (!positions) {
      positions = new Set<number>();
      index.set(value, positions);
    }
    positions.add(rowIndex);
  }

  private removeIndexEntry(
    table: TableData,
    column: string,
    value: Value,
    rowIndex: number,
  ): void {
    const index = table.indexes[column];
    if (!index || !this.isIndexableValue(value)) return;
    const positions = index.get(value);
    if (!positions) return;
    positions.delete(rowIndex);
    if (positions.size === 0) index.delete(value);
  }

  /**
   * Plan an index lookup for a WHERE clause.
   * Returns candidate row positions, or undefined when the predicate is not
   * indexable (caller falls back to a full scan). Supports `col = const`,
   * `const = col`, `col IN (...)`, and ANDs of indexable predicates.
   * Results are always re-checked with evaluateWhere, so a stale index entry
   * can only cost time, never correctness.
   */
  private planIndexLookup(
    table: TableData,
    where: WhereClause,
  ): number[] | undefined {
    return this.indexPlan(table, where)?.lookup();
  }

  /** One lazy plan drives both EXPLAIN and execution. Residual filters always run. */
  private indexPlan(
    table: TableData,
    where: WhereClause,
  ): { columns: string[]; lookup: () => number[] } | undefined {
    if (where.type === "and") {
      const left = this.indexPlan(table, where.left);
      const right = this.indexPlan(table, where.right);
      if (!left) return right;
      if (!right) return left;
      return {
        columns: [...new Set([...left.columns, ...right.columns])],
        lookup: () => {
          const a = left.lookup();
          const b = new Set(right.lookup());
          return a.filter((id) => b.has(id));
        },
      };
    }
    if (where.type !== "comparison") return undefined;
    const { left, right, operator } = where;
    let column: string | undefined;
    let values: Primitive[] = [];
    if (operator === "=" && !Array.isArray(right)) {
      if (
        left.type === "column_ref" &&
        right.type === "literal" &&
        this.isIndexableValue(right.value)
      ) {
        column = left.name;
        values = [right.value];
      } else if (
        right.type === "column_ref" &&
        left.type === "literal" &&
        this.isIndexableValue(left.value)
      ) {
        column = right.name;
        values = [left.value];
      }
    } else if (
      operator === "in" &&
      left.type === "column_ref" &&
      Array.isArray(right)
    ) {
      column = left.name;
      for (const item of right) {
        if (item.type !== "literal" || !this.isIndexableValue(item.value))
          return undefined;
        values.push(item.value);
      }
    }
    if (column === undefined || !table.indexes[column]) return undefined;
    const index = table.indexes[column];
    return {
      columns: [column],
      lookup: () => {
        const ids = new Set<number>();
        for (const value of values)
          for (const id of index.get(value) ?? []) ids.add(id);
        return [...ids];
      },
    };
  }

  private deleteRow(table: TableData, id: number, row: Row): void {
    for (const column of table.schema.columns)
      this.removeIndexEntry(table, column.name, row[column.name], id);
    if (table.schema.primaryKey)
      table.primaryIndex.delete(valueIdentity(row[table.schema.primaryKey]));
    this.storedBytes -= table.rowBytes.get(id) ?? 0;
    this.storedRows--;
    table.rowBytes.delete(id);
    table.rows.delete(id);
  }

  private assertColumnExists(
    table: TableData,
    column: string,
    tableName: string,
  ): void {
    if (!table.schema.columns.some((col) => col.name === column)) {
      throw new DatabaseError(
        `Column '${column}' not found in table '${tableName}'`,
        "COLUMN_NOT_FOUND",
      );
    }
  }

  private validateExpressionColumns(
    table: TableData,
    expression: Expression,
    tableName: string,
  ): void {
    if (expression.type === "column_ref") {
      this.assertColumnExists(table, expression.name, tableName);
    }
  }

  private validateWhereColumns(
    table: TableData,
    where: WhereClause,
    tableName: string,
  ): void {
    switch (where.type) {
      case "and":
        this.validateWhereColumns(table, where.left, tableName);
        this.validateWhereColumns(table, where.right, tableName);
        return;
      case "or":
        this.validateWhereColumns(table, where.left, tableName);
        this.validateWhereColumns(table, where.right, tableName);
        return;
      case "not":
        this.validateWhereColumns(table, where.clause, tableName);
        return;
      case "is_null":
      case "is_not_null":
        this.validateExpressionColumns(table, where.expression, tableName);
        return;
      case "comparison":
        this.validateExpressionColumns(table, where.left, tableName);
        if (Array.isArray(where.right)) {
          for (const expression of where.right) {
            this.validateExpressionColumns(table, expression, tableName);
          }
        } else {
          this.validateExpressionColumns(table, where.right, tableName);
        }
        return;
    }
  }

  private validateSelectReferences(
    table: TableData,
    columns: string[] | "*",
    orderBy: SelectStatement["orderBy"],
    where: WhereClause | undefined,
    tableName: string,
  ): void {
    if (columns !== "*") {
      for (const column of columns) {
        this.assertColumnExists(table, column, tableName);
      }
    }
    if (orderBy) {
      this.assertColumnExists(table, orderBy.column, tableName);
    }
    if (where) {
      this.validateWhereColumns(table, where, tableName);
    }
  }

  private coerceWriteValue(
    value: Value,
    column: ColumnDefinition,
    tableName: string,
    primaryKey = false,
  ): Value {
    if (value === null) {
      if (column.nullable === false || primaryKey) {
        const reason = primaryKey ? "Primary key" : `Column '${column.name}'`;
        throw new DatabaseError(
          `${reason} cannot be null`,
          primaryKey ? "PRIMARY_KEY_CONSTRAINT" : "NOT_NULL_CONSTRAINT",
        );
      }
      return null;
    }

    switch (column.type) {
      case "any":
        return cloneJsonValue(value, "SQL value");
      case "string":
        if (typeof value === "string") return value;
        break;
      case "number":
        if (typeof value === "number" && Number.isFinite(value)) return value;
        if (typeof value === "string" && value.trim() !== "") {
          const converted = Number(value);
          if (Number.isFinite(converted)) return converted;
        }
        break;
      case "boolean":
        if (typeof value === "boolean") return value;
        if (typeof value === "string") {
          if (value.toLowerCase() === "true") return true;
          if (value.toLowerCase() === "false") return false;
        }
        break;
    }

    throw new DatabaseError(
      `Invalid ${column.type} value for column '${column.name}' in table '${tableName}'`,
      "TYPE_CONSTRAINT",
    );
  }

  private assertPrimaryKeyUnique(
    table: TableData,
    candidates: Array<{ index?: number; row: Row }>,
  ): void {
    const primaryKey = table.schema.primaryKey;
    if (!primaryKey) return;

    const replacedRows = new Set(
      candidates.map((candidate) => candidate.index),
    );
    const seen = new Set<string>();
    for (const candidate of candidates) {
      const key = valueIdentity(candidate.row[primaryKey]);
      const existing = table.primaryIndex.get(key);
      if (
        seen.has(key) ||
        (existing !== undefined && !replacedRows.has(existing))
      ) {
        throw new DatabaseError(
          `Duplicate primary key value in table '${table.schema.name}'`,
          "PRIMARY_KEY_CONSTRAINT",
        );
      }
      seen.add(key);
    }
  }

  // Execute methods

  private executeCreateTable(statement: CreateTableStatement): QueryResult {
    const { tableName, columns, primaryKey } = statement;

    if (this.db.tables.has(tableName)) {
      throw new DatabaseError(
        `Table '${tableName}' already exists`,
        "TABLE_EXISTS",
      );
    }

    if (this.db.tables.size >= this.limits.maxTables)
      throw new DatabaseError("SQL table limit exceeded", "LIMIT_EXCEEDED");
    const tableData: TableData = {
      schema: {
        name: tableName,
        columns: columns.map((column) => ({
          ...column,
          ...(column.default === undefined
            ? {}
            : { default: cloneJsonValue(column.default) }),
        })),
        primaryKey,
      },
      rows: new Map(),
      nextRowId: 0,
      primaryIndex: new Map(),
      rowBytes: new Map(),
      indexes: Object.create(null),
    };

    // Create only configured indexes. The default configuration indexes all
    // columns; an explicit [] disables automatic indexes.
    for (const col of columns) {
      if (this.shouldIndexColumn(col.name)) {
        tableData.indexes[col.name] = new Map();
      }
    }

    this.db.tables.set(tableName, tableData);

    return {
      columns: [],
      rows: [],
      affectedRows: 0,
    };
  }

  private executeInsert(statement: InsertStatement): QueryResult {
    const { tableName, columns, values } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(
        `Table '${tableName}' not found`,
        "TABLE_NOT_FOUND",
      );
    }

    const colDefs = table.schema.columns;
    const insertColDefs: typeof colDefs = [];
    const seenColumns = new Set<string>();

    if (columns.length > 0) {
      for (const colName of columns) {
        if (seenColumns.has(colName)) {
          throw new DatabaseError(
            `Duplicate INSERT column '${colName}'`,
            "DUPLICATE_COLUMN",
          );
        }
        seenColumns.add(colName);

        const colDef = colDefs.find((c) => c.name === colName);
        if (!colDef) {
          throw new DatabaseError(
            `Column '${colName}' not found in table '${tableName}'`,
            "COLUMN_NOT_FOUND",
          );
        }
        insertColDefs.push(colDef);
      }
    } else {
      insertColDefs.push(...colDefs);
    }

    const preparedRows: Row[] = [];
    for (let rowIndex = 0; rowIndex < values.length; rowIndex++) {
      const rowValues = values[rowIndex];
      if (rowValues.length !== insertColDefs.length) {
        throw new DatabaseError(
          `Row ${rowIndex + 1} has ${rowValues.length} values; expected ${insertColDefs.length}`,
          "ROW_ARITY",
        );
      }

      const row: Row = Object.create(null);

      // Fill in values for specified columns or all columns in order
      for (let i = 0; i < insertColDefs.length; i++) {
        const colDef = insertColDefs[i];
        Object.defineProperty(row, colDef.name, {
          value: this.coerceWriteValue(
            rowValues[i],
            colDef,
            tableName,
            colDef.name === table.schema.primaryKey,
          ),
          enumerable: true,
          writable: true,
          configurable: true,
        });
      }

      // Fill in default values for unspecified columns
      for (const colDef of colDefs) {
        if (!Object.prototype.hasOwnProperty.call(row, colDef.name)) {
          if (colDef.default !== undefined) {
            row[colDef.name] = this.coerceWriteValue(
              colDef.default,
              colDef,
              tableName,
              colDef.name === table.schema.primaryKey,
            );
          } else {
            row[colDef.name] = this.coerceWriteValue(
              null,
              colDef,
              tableName,
              colDef.name === table.schema.primaryKey,
            );
          }
        }
      }

      preparedRows.push(row);
    }

    this.assertPrimaryKeyUnique(
      table,
      preparedRows.map((row) => ({ row })),
    );

    const sizes = preparedRows.map((row) =>
      Buffer.byteLength(JSON.stringify(row)),
    );
    const addedBytes = sizes.reduce((a, b) => a + b, 0);
    this.checkCapacity(
      this.storedRows + preparedRows.length,
      this.storedBytes + addedBytes,
    );
    if (!Number.isSafeInteger(table.nextRowId + preparedRows.length))
      throw new DatabaseError("SQL row ID limit exceeded", "LIMIT_EXCEEDED");
    for (let i = 0; i < preparedRows.length; i++) {
      const row = preparedRows[i];
      const id = table.nextRowId++;
      table.rows.set(id, row);
      table.rowBytes.set(id, sizes[i]);
      if (table.schema.primaryKey)
        table.primaryIndex.set(valueIdentity(row[table.schema.primaryKey]), id);
      for (const colDef of colDefs)
        this.addIndexEntry(table, colDef.name, row[colDef.name], id);
    }
    this.storedRows += preparedRows.length;
    this.storedBytes += addedBytes;

    return {
      columns: [],
      rows: [],
      affectedRows: preparedRows.length,
    };
  }

  private executeSelect(statement: SelectStatement): QueryResult {
    const { columns, tableName, where, orderBy, limit, offset } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(
        `Table '${tableName}' not found`,
        "TABLE_NOT_FOUND",
      );
    }
    this.validateSelectReferences(table, columns, orderBy, where, tableName);

    const skip = offset ?? 0;
    const take = limit ?? this.limits.maxResultRows + 1;
    const planned = where ? this.planIndexLookup(table, where) : undefined;
    const ids =
      planned === undefined ? table.rows.keys() : planned.sort((a, b) => a - b);
    type Selected = { row: Row; id: number };
    const compare = (a: Selected, b: Selected): number => {
      if (!orderBy) return a.id - b.id;
      const av = a.row[orderBy.column];
      const bv = b.row[orderBy.column];
      const cmp = this.compareOrderValues(av, bv);
      return (
        (cmp && av != null && bv != null && orderBy.direction === "desc"
          ? -cmp
          : cmp) || a.id - b.id
      );
    };
    const k = Math.min(table.rows.size, skip + take);
    const top = orderBy ? new TopK<Selected>(k, compare) : undefined;
    let selected: Selected[] = [];
    let matched = 0;
    this.examinedRows = 0;
    if (take > 0)
      for (const id of ids) {
        const row = table.rows.get(id);
        if (!row) continue;
        this.examinedRows++;
        if (where && this.evaluateWhere(row, where, table) !== true) continue;
        if (top) top.add({ row, id });
        else {
          if (matched++ < skip) continue;
          selected.push({ row, id });
          if (selected.length >= take) break;
        }
      }
    if (top) selected = top.sorted().slice(skip, skip + take);
    if (selected.length > this.limits.maxResultRows)
      throw new DatabaseError(
        "SQL result row limit exceeded",
        "LIMIT_EXCEEDED",
      );
    const rows = selected.map((item) => item.row);

    // Select columns
    const selectedColumns =
      columns === "*" ? table.schema.columns.map((c) => c.name) : columns;

    let resultBytes = 2;
    const resultRows = rows.map((row) => {
      const resultRow: Row = {};
      for (const col of selectedColumns) {
        resultBytes +=
          Buffer.byteLength(JSON.stringify(col)) +
          Buffer.byteLength(JSON.stringify(row[col] ?? null)) +
          2;
      }
      resultBytes += 3;
      if (resultBytes > this.limits.maxResultBytes)
        throw new DatabaseError(
          "SQL result byte limit exceeded",
          "LIMIT_EXCEEDED",
        );
      for (const col of selectedColumns) {
        Object.defineProperty(resultRow, col, {
          value: cloneJsonValue(row[col] ?? null, "SQL result"),
          enumerable: true,
          writable: true,
          configurable: true,
        });
      }
      return resultRow;
    });

    return {
      columns: selectedColumns,
      rows: resultRows,
    };
  }

  private executeUpdate(statement: UpdateStatement): QueryResult {
    const { tableName, set, where } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(
        `Table '${tableName}' not found`,
        "TABLE_NOT_FOUND",
      );
    }
    if (where) this.validateWhereColumns(table, where, tableName);

    const setColumns = new Set<string>();
    const preparedSet: Array<{ column: string; value: Value }> = [];
    for (const { column, value } of set) {
      if (setColumns.has(column)) {
        throw new DatabaseError(
          `Duplicate UPDATE column '${column}'`,
          "DUPLICATE_COLUMN",
        );
      }
      setColumns.add(column);

      const colDef = table.schema.columns.find((c) => c.name === column);
      if (!colDef) {
        throw new DatabaseError(
          `Column '${column}' not found in table '${tableName}'`,
          "COLUMN_NOT_FOUND",
        );
      }

      preparedSet.push({
        column,
        value: this.coerceWriteValue(
          value,
          colDef,
          tableName,
          column === table.schema.primaryKey,
        ),
      });
    }

    const candidateIndices = where
      ? this.planIndexLookup(table, where)
      : undefined;
    const rowIndices = candidateIndices ?? table.rows.keys();
    const updates: Array<{ index: number; row: Row }> = [];

    for (const i of rowIndices) {
      const row = table.rows.get(i);
      if (!row) continue;

      // Check WHERE condition
      if (where && this.evaluateWhere(row, where, table) !== true) {
        continue;
      }

      const nextRow: Row = Object.assign(Object.create(null), row);
      for (const { column, value } of preparedSet) {
        nextRow[column] = value;
      }
      updates.push({ index: i, row: nextRow });
    }

    const pk = table.schema.primaryKey;
    const changesPk = pk !== undefined && setColumns.has(pk);
    if (changesPk) this.assertPrimaryKeyUnique(table, updates);
    const sizes = updates.map((update) =>
      Buffer.byteLength(JSON.stringify(update.row)),
    );
    const delta = updates.reduce(
      (total, update, i) =>
        total + sizes[i] - (table.rowBytes.get(update.index) ?? 0),
      0,
    );
    this.checkCapacity(this.storedRows, this.storedBytes + delta);
    if (changesPk)
      for (const update of updates)
        table.primaryIndex.delete(
          valueIdentity(table.rows.get(update.index)![pk!]),
        );
    for (let i = 0; i < updates.length; i++) {
      const update = updates[i];
      const current = table.rows.get(update.index)!;
      for (const { column } of preparedSet) {
        this.removeIndexEntry(table, column, current[column], update.index);
        this.addIndexEntry(table, column, update.row[column], update.index);
      }
      table.rows.set(update.index, update.row);
      table.rowBytes.set(update.index, sizes[i]);
      if (changesPk)
        table.primaryIndex.set(valueIdentity(update.row[pk!]), update.index);
    }
    this.storedBytes += delta;

    return {
      columns: [],
      rows: [],
      affectedRows: updates.length,
    };
  }

  private executeDelete(statement: DeleteStatement): QueryResult {
    const { tableName, where } = statement;

    const table = this.db.tables.get(tableName);
    if (!table) {
      throw new DatabaseError(
        `Table '${tableName}' not found`,
        "TABLE_NOT_FOUND",
      );
    }
    if (where) this.validateWhereColumns(table, where, tableName);

    const ids =
      (where ? this.planIndexLookup(table, where) : undefined) ??
      table.rows.keys();
    const deleted: Array<[number, Row]> = [];
    // Stage predicate evaluation: a work-limit failure must not partially delete.
    for (const id of ids) {
      const row = table.rows.get(id);
      if (row && (!where || this.evaluateWhere(row, where, table) === true))
        deleted.push([id, row]);
    }
    for (const [id, row] of deleted) this.deleteRow(table, id, row);
    const affectedRows = deleted.length;

    return {
      columns: [],
      rows: [],
      affectedRows,
    };
  }

  private executeDropTable(statement: DropTableStatement): QueryResult {
    const { tableName } = statement;

    if (!this.db.tables.has(tableName)) {
      throw new DatabaseError(
        `Table '${tableName}' not found`,
        "TABLE_NOT_FOUND",
      );
    }

    const table = this.db.tables.get(tableName)!;
    this.storedRows -= table.rows.size;
    for (const bytes of table.rowBytes.values()) this.storedBytes -= bytes;
    this.db.tables.delete(tableName);

    return {
      columns: [],
      rows: [],
      affectedRows: 0,
    };
  }

  private evaluateWhere(
    row: Row,
    where: WhereClause,
    table: TableData,
  ): TruthValue {
    switch (where.type) {
      case "and": {
        const left = this.evaluateWhere(row, (where as AndClause).left, table);
        const right = this.evaluateWhere(
          row,
          (where as AndClause).right,
          table,
        );
        if (left === false || right === false) return false;
        if (left === null || right === null) return null;
        return true;
      }
      case "or": {
        const left = this.evaluateWhere(row, (where as OrClause).left, table);
        const right = this.evaluateWhere(row, (where as OrClause).right, table);
        if (left === true || right === true) return true;
        if (left === null || right === null) return null;
        return false;
      }
      case "not": {
        const value = this.evaluateWhere(
          row,
          (where as NotClause).clause,
          table,
        );
        return value === null ? null : !value;
      }
      case "is_null":
      case "is_not_null": {
        const expression = (where as IsNullClause).expression;
        const value = this.evaluateExpression(row, expression);
        const isNull = value === null || value === undefined;
        return where.type === "is_null" ? isNull : !isNull;
      }
      case "comparison":
        return this.evaluateComparison(row, where as ComparisonClause, table);
      default:
        return null;
    }
  }

  private valuesEqual(a: Value, b: Value): boolean {
    return structuralEqual(a, b);
  }

  private evaluateExpression(row: Row, expression: Expression): Value {
    if (expression.type === "column_ref") {
      return row[expression.name];
    }
    return expression.value;
  }

  private evaluateComparison(
    row: Row,
    comp: ComparisonClause,
    _table: TableData,
  ): TruthValue {
    const { left, operator, right } = comp;

    const leftValue = this.evaluateExpression(row, left);
    let rightValue: Value | Value[];
    if (Array.isArray(right)) {
      rightValue = right.map((expression) =>
        this.evaluateExpression(row, expression),
      );
    } else {
      rightValue = this.evaluateExpression(row, right);
    }

    // Ordinary comparisons follow SQL's three-valued NULL behavior: any
    // comparison involving NULL is UNKNOWN, not true or false. WHERE only
    // keeps predicates whose final truth value is TRUE.
    if (leftValue === null || leftValue === undefined) return null;
    if (Array.isArray(rightValue)) {
      if (rightValue.length === 0) return false;
      if (operator === "in") {
        let hasUnknown = false;
        for (const value of rightValue) {
          if (value === null || value === undefined) {
            hasUnknown = true;
          } else if (this.valuesEqual(leftValue, value)) {
            return true;
          }
        }
        return hasUnknown ? null : false;
      }
      if (operator === "between") {
        const [low, high] = rightValue;
        const lower =
          low == null ? null : this.compareValues(leftValue, low) >= 0;
        const upper =
          high == null ? null : this.compareValues(leftValue, high) <= 0;
        if (lower === false || upper === false) return false;
        return lower === null || upper === null ? null : true;
      }
    } else if (rightValue === null || rightValue === undefined) {
      return null;
    }

    switch (operator) {
      case "=":
        return (
          !Array.isArray(rightValue) && this.valuesEqual(leftValue, rightValue)
        );
      case "!=":
        return (
          !Array.isArray(rightValue) && !this.valuesEqual(leftValue, rightValue)
        );
      case ">":
        return this.compareValues(leftValue, rightValue) > 0;
      case ">=":
        return this.compareValues(leftValue, rightValue) >= 0;
      case "<":
        return this.compareValues(leftValue, rightValue) < 0;
      case "<=":
        return this.compareValues(leftValue, rightValue) <= 0;
      case "like":
        return this.likeCompare(String(leftValue), String(rightValue));
      case "in":
        return false;
      case "between":
        if (Array.isArray(rightValue) && rightValue.length === 2) {
          const [low, high] = rightValue;
          return (
            this.compareValues(leftValue, low) >= 0 &&
            this.compareValues(leftValue, high) <= 0
          );
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

    if (typeof a === "number" && typeof b === "number") {
      return a - b;
    }

    return String(a).localeCompare(String(b));
  }

  private compareOrderValues(
    a: Value | undefined,
    b: Value | undefined,
  ): number {
    const aNull = a === null || a === undefined;
    const bNull = b === null || b === undefined;
    if (aNull || bNull) {
      if (aNull && bNull) return 0;
      return aNull ? 1 : -1;
    }

    const aRank = this.orderTypeRank(a);
    const bRank = this.orderTypeRank(b);
    if (aRank !== bRank) return aRank - bRank;

    if (typeof a === "number" && typeof b === "number") {
      const aFinite = Number.isFinite(a);
      const bFinite = Number.isFinite(b);
      if (aFinite !== bFinite) return aFinite ? -1 : 1;
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }

    if (typeof a === "string" && typeof b === "string") {
      return a.localeCompare(b);
    }

    if (typeof a === "boolean" && typeof b === "boolean") {
      return a === b ? 0 : a ? 1 : -1;
    }

    try {
      return JSON.stringify(a).localeCompare(JSON.stringify(b));
    } catch {
      return String(a).localeCompare(String(b));
    }
  }

  private orderTypeRank(value: Value): number {
    if (typeof value === "number") return 0;
    if (typeof value === "string") return 1;
    if (typeof value === "boolean") return 2;
    if (Array.isArray(value)) return 3;
    return 4;
  }

  private likeCompare(value: string, pattern: string): boolean {
    return matchLike(value, pattern);
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
    const statement = this.prepared(text);
    if (statement.type !== "select") {
      return { statement: statement.type, strategy: "n/a", hasOrderBy: false };
    }
    const table = this.db.tables.get(statement.tableName);
    if (table) {
      this.validateSelectReferences(
        table,
        statement.columns,
        statement.orderBy,
        statement.where,
        statement.tableName,
      );
    }
    const plan: QueryPlan = {
      statement: "select",
      table: statement.tableName,
      columns: statement.columns === "*" ? "*" : [...statement.columns],
      strategy: "full-scan",
      hasOrderBy: statement.orderBy !== undefined,
      tableRows: table?.rows.size,
    };
    if (statement.orderBy !== undefined) {
      plan.orderBy = {
        column: statement.orderBy.column,
        direction: statement.orderBy.direction,
      };
    }
    if (statement.limit !== undefined) plan.limit = statement.limit;
    if (statement.offset !== undefined) plan.offset = statement.offset;
    if (table && statement.where) {
      const cols = this.describeIndexUse(table, statement.where);
      if (cols) {
        plan.strategy = "index-scan";
        plan.indexColumns = cols;
      }
    } else if (table && !statement.where) {
      plan.strategy = "full-scan";
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
      ...(result.affectedRows === undefined
        ? {}
        : { affectedRows: result.affectedRows }),
    };
  }

  /**
   * Columns an index lookup would use for this WHERE (mirrors
   * planIndexLookup). Returns undefined when it falls back to a full scan.
   */
  private describeIndexUse(
    table: TableData,
    where: WhereClause,
  ): string[] | undefined {
    return this.indexPlan(table, where)?.columns;
  }

  getTableNames(): string[] {
    return Array.from(this.db.tables.keys());
  }

  getTableSchema(tableName: string) {
    const table = this.db.tables.get(tableName);
    if (!table) return undefined;
    return {
      ...table.schema,
      columns: table.schema.columns.map((column) => ({
        ...column,
        ...(column.default === undefined
          ? {}
          : { default: cloneJsonValue(column.default, "column default") }),
      })),
    };
  }

  reset(): void {
    this.db = { tables: new Map() };
    this.storedRows = 0;
    this.storedBytes = 0;
    this.examinedRows = 0;
    this.statements.clear();
    this.statementBytes = 0;
  }
}
