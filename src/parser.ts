// SQL Parser for YASD
// Parses SQL-like queries into AST

import {
  SqlStatement,
  CreateTableStatement,
  InsertStatement,
  SelectStatement,
  UpdateStatement,
  DeleteStatement,
  DropTableStatement,
  ColumnDefinition,
  WhereClause,
  ComparisonClause,
  Expression,
  AndClause,
  OrClause,
  NotClause,
  OrderByClause,
  Value,
} from './types';

const NUMERIC_LITERAL = /^-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const NUMERIC_TOKEN_CHAR = /[0-9.eE+-]/;
const IDENTIFIER_CHAR = /[a-zA-Z0-9_]/;

interface ParserState {
  sql: string;
  pos: number;
  currentToken: string | null;
}

class Parser {
  private state: ParserState;

  constructor(sql: string) {
    this.state = {
      sql: sql.trim(),
      pos: 0,
      currentToken: null
    };
    this.nextToken();
  }

  private nextToken(): string | null {
    // Skip whitespace
    while (this.state.pos < this.state.sql.length && /\s/.test(this.state.sql[this.state.pos])) {
      this.state.pos++;
    }
    
    if (this.state.pos >= this.state.sql.length) {
      this.state.currentToken = null;
      return null;
    }

    // Check for quoted strings. Backslash escapes and doubled quote
    // delimiters are decoded here; parseValue only unwraps the token.
    if (this.state.sql[this.state.pos] === "'" || this.state.sql[this.state.pos] === '"') {
      const quote = this.state.sql[this.state.pos];
      this.state.pos++;
      let str = '';
      let closed = false;
      while (this.state.pos < this.state.sql.length) {
        const c = this.state.sql[this.state.pos];
        if (c === quote) {
          if (this.state.sql[this.state.pos + 1] === quote) {
            str += quote;
            this.state.pos += 2;
            continue;
          }
          this.state.pos++;
          closed = true;
          break;
        }
        if (c === '\\') {
          this.state.pos++;
          if (this.state.pos >= this.state.sql.length) {
            throw new Error('Unterminated quoted string');
          }
          const escaped = this.state.sql[this.state.pos++];
          switch (escaped) {
            case '0': str += '\0'; break;
            case 'b': str += '\b'; break;
            case 'f': str += '\f'; break;
            case 'n': str += '\n'; break;
            case 'r': str += '\r'; break;
            case 't': str += '\t'; break;
            case 'v': str += '\v'; break;
            case '\\': str += '\\'; break;
            case "'": str += "'"; break;
            case '"': str += '"'; break;
            default:
              throw new Error(`Unsupported escape sequence \\${escaped}`);
          }
          continue;
        }
        str += c;
        this.state.pos++;
      }
      if (!closed) throw new Error('Unterminated quoted string');
      this.state.currentToken = quote + str + quote;
      return this.state.currentToken;
    }

    // Scan the entire numeric-looking run so malformed values such as
    // `10oops`, `1.2.3`, and `1e+` cannot be split into valid tokens.
    const first = this.state.sql[this.state.pos];
    const next = this.state.sql[this.state.pos + 1];
    const afterNext = this.state.sql[this.state.pos + 2];
    const numericStart =
      /\d/.test(first) ||
      (first === '.' && /\d/.test(next)) ||
      (first === '-' && (/\d/.test(next) || (next === '.' && /\d/.test(afterNext))));
    if (numericStart) {
      const start = this.state.pos;
      while (
        this.state.pos < this.state.sql.length &&
        NUMERIC_TOKEN_CHAR.test(this.state.sql[this.state.pos])
      ) {
        this.state.pos++;
      }
      const numStr = this.state.sql.slice(start, this.state.pos);
      if (
        (this.state.pos < this.state.sql.length &&
          IDENTIFIER_CHAR.test(this.state.sql[this.state.pos])) ||
        !NUMERIC_LITERAL.test(numStr)
      ) {
        throw new Error(`Malformed numeric literal '${numStr}'`);
      }
      this.state.currentToken = numStr;
      return numStr;
    }

    // Check for operators and punctuation (longest match first; word
    // operators need a word boundary so `likely` isn't lexed as `like`)
    const symbolOps = ['>=', '<=', '!=', '=', '>', '<', '(', ')', ',', '*', ';'];
    for (const op of symbolOps) {
      if (this.state.sql.slice(this.state.pos, this.state.pos + op.length) === op) {
        this.state.pos += op.length;
        this.state.currentToken = op;
        return op;
      }
    }
    const wordOps = ['like', 'between', 'in', 'and', 'or', 'not', 'is'];
    for (const op of wordOps) {
      const candidate = this.state.sql.slice(this.state.pos, this.state.pos + op.length);
      const after = this.state.sql[this.state.pos + op.length] ?? '';
      if (candidate.toLowerCase() === op && !/[a-zA-Z0-9_]/.test(after)) {
        // `in`/`or` etc. must not match mid-identifier; also check the char
        // before (we always tokenize left-to-right, so only `after` matters
        // for the prefix case, but a preceding word char means we're inside
        // an identifier — that can't happen here since identifiers are
        // consumed greedily below; still, keep the check cheap and safe).
        this.state.pos += op.length;
        this.state.currentToken = op;
        return op;
      }
    }

    // Check for keywords and identifiers (first char only — the old
    // unanchored test against the whole remainder swallowed spaces,
    // commas and the rest of the query into one "identifier")
    if (/[a-zA-Z_]/.test(first)) {
      let ident = '';
      while (this.state.pos < this.state.sql.length && IDENTIFIER_CHAR.test(this.state.sql[this.state.pos])) {
        ident += this.state.sql[this.state.pos];
        this.state.pos++;
      }

      if (ident) {
        this.state.currentToken = ident;
        return ident;
      }
    }

    throw new Error(`Unexpected character '${first}' at position ${this.state.pos}`);
  }

  private consume(expected: string | null = null): string {
    const token = this.state.currentToken;
    if (expected && token?.toLowerCase() !== expected.toLowerCase()) {
      throw new Error(`Expected '${expected}', got '${token}'`);
    }
    this.nextToken();
    return token || '';
  }

  private peek(): string | null {
    return this.state.currentToken;
  }

  private expectIdentifier(): string {
    const token = this.peek();
    if (!token || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token)) {
      throw new Error(`Expected identifier, got '${token}'`);
    }
    return this.consume();
  }

  private parseType(): 'string' | 'number' | 'boolean' | 'any' {
    const token = this.peek()?.toLowerCase();
    switch (token) {
      case 'varchar':
      case 'text':
      case 'string':
        this.consume();
        return 'string';
      case 'int':
      case 'integer':
      case 'number':
      case 'float':
      case 'decimal':
      case 'numeric':
        this.consume();
        return 'number';
      case 'bool':
      case 'boolean':
        this.consume();
        return 'boolean';
      case 'any':
        this.consume();
        return 'any';
      default:
        throw new Error(`Unknown column type '${this.peek()}'`);
    }
  }

  private validateCreateTableSchema(columns: ColumnDefinition[], primaryKey: string | undefined): void {
    if (columns.length === 0) {
      throw new Error('CREATE TABLE requires at least one column');
    }

    const columnNames = new Set<string>();
    for (const column of columns) {
      if (columnNames.has(column.name)) {
        throw new Error(`Duplicate column '${column.name}'`);
      }
      columnNames.add(column.name);
    }

    if (primaryKey !== undefined && !columnNames.has(primaryKey)) {
      throw new Error(`Primary key column '${primaryKey}' does not exist`);
    }
  }

  private parseValue(): Value {
    const token = this.peek();
    
    if (token === null) {
      throw new Error('Unexpected end of input');
    }

    const normalized = token.toLowerCase();
    if (normalized === 'null') {
      this.consume();
      return null;
    }

    if (normalized === 'true') {
      this.consume();
      return true;
    }

    if (normalized === 'false') {
      this.consume();
      return false;
    }

    if (NUMERIC_LITERAL.test(token)) {
      const num = Number(token);
      this.consume();
      if (!Number.isFinite(num)) throw new Error(`Numeric literal out of range '${token}'`);
      return num;
    }

    if ((token.startsWith("'") && token.endsWith("'") && token.length >= 2) ||
        (token.startsWith('"') && token.endsWith('"') && token.length >= 2)) {
      const val = token.slice(1, -1);
      this.consume();
      return val;
    }

    // Could be an identifier (column name)
    if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token)) {
      this.consume();
      return token;
    }

    throw new Error(`Unexpected value: ${token}`);
  }

  private parseExpression(): Expression {
    const token = this.peek();
    if (token === null) {
      throw new Error('Unexpected end of input');
    }
    if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token) &&
        !['null', 'true', 'false'].includes(token.toLowerCase())) {
      this.consume();
      return { type: 'column_ref', name: token };
    }
    return { type: 'literal', value: this.parseValue() };
  }

  private parseInteger(name: string): number {
    const token = this.peek();
    if (token === null || !/^-?\d+$/.test(token)) {
      throw new Error(`${name} must be an integer, got '${token}'`);
    }
    const value = Number(token);
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`${name} must be a non-negative safe integer, got '${token}'`);
    }
    this.consume();
    return value;
  }

  parse(): SqlStatement {
    const keyword = this.peek()?.toLowerCase();
    let statement: SqlStatement;
    switch (keyword) {
      case 'create':
        statement = this.parseCreateTable();
        break;
      case 'insert':
        statement = this.parseInsert();
        break;
      case 'select':
        statement = this.parseSelect();
        break;
      case 'update':
        statement = this.parseUpdate();
        break;
      case 'delete':
        statement = this.parseDelete();
        break;
      case 'drop':
        statement = this.parseDropTable();
        break;
      default:
        throw new Error(`Unexpected keyword: ${keyword}`);
    }
    if (this.peek() === ';') this.consume();
    if (this.peek() !== null) {
      throw new Error(`Unexpected trailing token '${this.peek()}'`);
    }
    return statement;
  }

  private parseCreateTable(): CreateTableStatement {
    this.consume('create');
    this.consume('table');
    
    const tableName = this.expectIdentifier();
    
    this.consume('(');
    
    const columns: ColumnDefinition[] = [];
    let primaryKey: string | undefined;
    let primaryKeyDeclarations = 0;
    
    while (this.peek() !== ')') {
      // Table-level `PRIMARY KEY (col)` constraint (no column definition).
      if (this.peek()?.toLowerCase() === 'primary') {
        if (primaryKeyDeclarations > 0) {
          throw new Error('Multiple primary key declarations are not allowed');
        }
        this.consume();
        this.consume('key');
        this.consume('(');
        primaryKey = this.expectIdentifier();
        this.consume(')');
        primaryKeyDeclarations++;
        if (this.peek() === ',') {
          this.consume();
        }
        continue;
      }

      const columnName = this.expectIdentifier();
      if (columns.some(column => column.name === columnName)) {
        throw new Error(`Duplicate column '${columnName}'`);
      }
      const columnType = this.parseType();

      // Sized declarations are accepted as metadata, not truncation/coercion.
      if (this.peek() === '(') {
        this.consume('(');
        const precision = this.parseInteger('type size');
        if (precision === 0) throw new Error('type size must be positive');
        if (this.peek() === ',') {
          if (columnType !== 'number') throw new Error('scale requires a numeric type');
          this.consume(',');
          const scale = this.parseInteger('type scale');
          if (scale > precision) throw new Error('type scale exceeds precision');
        }
        this.consume(')');
      }
      
      let nullable = true;
      let defaultValue: Value | undefined = undefined;
      
      // Parse column modifiers
      while (true) {
        const next = this.peek()?.toLowerCase();
        if (next === 'not') {
          this.consume();
          this.consume('null');
          nullable = false;
        } else if (next === 'null') {
          this.consume();
          nullable = true;
        } else if (next === 'default') {
          this.consume();
          defaultValue = this.parseValue();
        } else if (next === 'primary') {
          if (primaryKeyDeclarations > 0) {
            throw new Error('Multiple primary key declarations are not allowed');
          }
          this.consume();
          this.consume('key');
          primaryKey = columnName;
          primaryKeyDeclarations++;
        } else {
          break;
        }
      }
      
      columns.push({ name: columnName, type: columnType, nullable, default: defaultValue });
      
      if (this.peek() === ',') {
        this.consume();
      } else if (this.peek() !== ')') {
        throw new Error(`Expected ',' or ')', got '${this.peek()}'`);
      }
    }
    
    this.consume(')');
    

    this.validateCreateTableSchema(columns, primaryKey);
    
    return { type: 'create_table', tableName, columns, primaryKey };
  }

  private parseInsert(): InsertStatement {
    this.consume('insert');
    this.consume('into');
    
    const tableName = this.expectIdentifier();
    
    let columns: string[] = [];
    let values: Value[][] = [];
    
    if (this.peek() === '(') {
      this.consume('(');
      columns = [];
      while (this.peek() !== ')') {
        columns.push(this.expectIdentifier());
        if (this.peek() === ',') {
          this.consume();
        }
      }
      this.consume(')');
    }
    
    this.consume('values');
    this.consume('(');
    
    const rowValues: Value[] = [];
    while (this.peek() !== ')') {
      rowValues.push(this.parseValue());
      if (this.peek() === ',') {
        this.consume();
      }
    }
    this.consume(')');
    
    values = [rowValues];
    
    // Handle multiple value sets
    while (this.peek() === ',') {
      this.consume();
      this.consume('(');
      const nextRow: Value[] = [];
      while (this.peek() !== ')') {
        nextRow.push(this.parseValue());
        if (this.peek() === ',') {
          this.consume();
        }
      }
      this.consume(')');
      values.push(nextRow);
    }
    
    
    return { type: 'insert', tableName, columns, values };
  }

  private parseSelect(): SelectStatement {
    this.consume('select');
    
    let columns: string[] | '*' = '*';
    
    if (this.peek() !== '*') {
      const cols: string[] = [];
      cols.push(this.expectIdentifier());
      while (this.peek() === ',') {
        this.consume();
        cols.push(this.expectIdentifier());
      }
      columns = cols;
    } else {
      this.consume('*');
    }
    
    this.consume('from');
    const tableName = this.expectIdentifier();
    
    let where: WhereClause | undefined;
    if (this.peek()?.toLowerCase() === 'where') {
      this.consume();
      where = this.parseWhereClause();
    }
    
    let orderBy: OrderByClause | undefined;
    if (this.peek()?.toLowerCase() === 'order') {
      this.consume();
      this.consume('by');
      const column = this.expectIdentifier();
      let direction: 'asc' | 'desc' = 'asc';
      if (this.peek()?.toLowerCase() === 'asc' || this.peek()?.toLowerCase() === 'desc') {
        direction = this.consume().toLowerCase() as 'asc' | 'desc';
      }
      orderBy = { column, direction };
    }
    
    let limit: number | undefined;
    if (this.peek()?.toLowerCase() === 'limit') {
      this.consume();
      limit = this.parseInteger('LIMIT');
    }
    
    let offset: number | undefined;
    if (this.peek()?.toLowerCase() === 'offset') {
      this.consume();
      offset = this.parseInteger('OFFSET');
    }
    
    
    return { type: 'select', columns, tableName, where, orderBy, limit, offset };
  }

  private parseUpdate(): UpdateStatement {
    this.consume('update');
    const tableName = this.expectIdentifier();
    
    this.consume('set');
    
    const setClauses: { column: string; value: Value }[] = [];
    
    for (;;) {
      const column = this.expectIdentifier();
      this.consume('=');
      const value = this.parseValue();
      setClauses.push({ column, value });
      if (this.peek() !== ',') break;
      this.consume(',');
    }
    
    let where: WhereClause | undefined;
    if (this.peek()?.toLowerCase() === 'where') {
      this.consume();
      where = this.parseWhereClause();
    }
    
    
    return { type: 'update', tableName, set: setClauses, where };
  }

  private parseDelete(): DeleteStatement {
    this.consume('delete');
    this.consume('from');
    const tableName = this.expectIdentifier();
    
    let where: WhereClause | undefined;
    if (this.peek()?.toLowerCase() === 'where') {
      this.consume();
      where = this.parseWhereClause();
    }
    
    
    return { type: 'delete', tableName, where };
  }

  private parseDropTable(): DropTableStatement {
    this.consume('drop');
    this.consume('table');
    const tableName = this.expectIdentifier();
    
    
    return { type: 'drop_table', tableName };
  }

  private parseWhereClause(): WhereClause {
    return this.parseOrClause();
  }

  private parseOrClause(): WhereClause {
    let left = this.parseAndClause();
    
    while (this.peek()?.toLowerCase() === 'or') {
      this.consume();
      const right = this.parseAndClause();
      left = { type: 'or', left, right };
    }
    
    return left;
  }

  private parseAndClause(): WhereClause {
    let left = this.parseNotClause();
    
    while (this.peek()?.toLowerCase() === 'and') {
      this.consume();
      const right = this.parseNotClause();
      left = { type: 'and', left, right };
    }
    
    return left;
  }

  private parseNotClause(): WhereClause {
    if (this.peek()?.toLowerCase() === 'not') {
      this.consume();
      const clause = this.parseNotClause();
      return { type: 'not', clause };
    }
    
    if (this.peek() === '(') {
      this.consume('(');
      const clause = this.parseWhereClause();
      this.consume(')');
      return clause;
    }
    return this.parseComparisonClause();
  }

  private parseComparisonClause(): WhereClause {
    const left = this.parseExpression();

    const op = this.peek()?.toLowerCase();
    if (!op || !['=', '!=', '>', '>=', '<', '<=', 'like', 'in', 'between', 'is'].includes(op)) {
      throw new Error(`Expected comparison operator, got '${op}'`);
    }
    this.consume();

    if (op === 'is') {
      const isNotNull = this.peek()?.toLowerCase() === 'not';
      if (isNotNull) this.consume();
      this.consume('null');
      return { type: isNotNull ? 'is_not_null' : 'is_null', expression: left };
    }

    // `IN (...)` and `BETWEEN a AND b` take structured operands — parse them
    // before the generic single-value path (the old code called parseValue()
    // first, which choked on the opening paren).
    let right: Expression | Expression[];
    if (op === 'in') {
      this.consume('(');
      const values: Expression[] = [];
      values.push(this.parseExpression());
      while (this.peek() === ',') {
        this.consume();
        values.push(this.parseExpression());
      }
      this.consume(')');
      right = values;
    } else if (op === 'between') {
      const low = this.parseExpression();
      this.consume('and');
      const high = this.parseExpression();
      right = [low, high];
    } else {
      right = this.parseExpression();
    }

    return {
      type: 'comparison',
      left,
      operator: op as ComparisonClause['operator'],
      right
    };
  }
}

import { DatabaseError } from './errors';

export function parse(sql: string): SqlStatement {
  try {
    if (typeof sql !== 'string' || Buffer.byteLength(sql, 'utf8') > 4 * 1024 * 1024) {
      throw new Error('SQL must be a string no larger than 4 MiB');
    }
    const parser = new Parser(sql);
    return parser.parse();
  } catch (error) {
    if (error instanceof DatabaseError) throw error;
    throw new DatabaseError(error instanceof Error ? error.message : String(error), 'PARSE_ERROR', { cause: error });
  }
}
