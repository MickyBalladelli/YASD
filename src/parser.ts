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
  OrderByClause
} from './types';

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

    // Check for quoted strings (keep the quotes in the token so parseValue
    // can tell string literals apart from identifiers/numbers)
    if (this.state.sql[this.state.pos] === "'" || this.state.sql[this.state.pos] === '"') {
      const quote = this.state.sql[this.state.pos];
      this.state.pos++;
      let str = '';
      while (this.state.pos < this.state.sql.length && this.state.sql[this.state.pos] !== quote) {
        if (this.state.sql[this.state.pos] === '\\') {
          this.state.pos++;
          str += this.state.sql[this.state.pos];
        } else {
          str += this.state.sql[this.state.pos];
        }
        this.state.pos++;
      }
      this.state.pos++; // skip closing quote
      this.state.currentToken = quote + str + quote;
      return this.state.currentToken;
    }

    // Check for numbers
    if (/[\d.-]/.test(this.state.sql[this.state.pos])) {
      let numStr = '';
      let hasDot = false;
      while (this.state.pos < this.state.sql.length) {
        const c = this.state.sql[this.state.pos];
        if (/\d/.test(c)) {
          numStr += c;
          this.state.pos++;
        } else if (c === '.' && !hasDot) {
          numStr += c;
          hasDot = true;
          this.state.pos++;
        } else if (c === '-') {
          numStr += c;
          this.state.pos++;
        } else {
          break;
        }
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
    const wordOps = ['like', 'between', 'in', 'and', 'or', 'not'];
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
    const first = this.state.sql[this.state.pos];
    if (/[a-zA-Z_]/.test(first)) {
      let ident = '';
      while (this.state.pos < this.state.sql.length && /[a-zA-Z0-9_]/.test(this.state.sql[this.state.pos])) {
        ident += this.state.sql[this.state.pos];
        this.state.pos++;
      }

      if (ident) {
        this.state.currentToken = ident;
        return ident;
      }
    }

    this.state.currentToken = null;
    return null;
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
      default:
        this.consume();
        return 'any';
    }
  }

  private parseValue(): any {
    const token = this.peek();
    
    if (token === null) {
      throw new Error('Unexpected end of input');
    }

    if (token === 'null') {
      this.consume();
      return null;
    }

    if (token === 'true') {
      this.consume();
      return true;
    }

    if (token === 'false') {
      this.consume();
      return false;
    }

    if (/^-?(\d+(\.\d+)?|\.\d+)$/.test(token)) {
      const num = parseFloat(token);
      this.consume();
      return isNaN(num) ? token : num;
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
    
    while (this.peek() !== ')') {
      // Table-level `PRIMARY KEY (col)` constraint (no column definition).
      if (this.peek()?.toLowerCase() === 'primary') {
        this.consume();
        this.consume('key');
        this.consume('(');
        primaryKey = this.expectIdentifier();
        this.consume(')');
        if (this.peek() === ',') {
          this.consume();
        }
        continue;
      }

      const columnName = this.expectIdentifier();
      const columnType = this.parseType();

      // Optional sized type params, e.g. varchar(255) / numeric(10,2).
      if (this.peek() === '(') {
        this.consume('(');
        while (this.peek() !== null && this.peek() !== ')') {
          this.consume();
        }
        this.consume(')');
      }
      
      let nullable = true;
      let defaultValue: any = undefined;
      
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
          this.consume();
          this.consume('key');
          primaryKey = columnName;
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
    
    // Optional semicolon
    if (this.peek() === ';') {
      this.consume();
    }
    
    return { type: 'create_table', tableName, columns, primaryKey };
  }

  private parseInsert(): InsertStatement {
    this.consume('insert');
    this.consume('into');
    
    const tableName = this.expectIdentifier();
    
    let columns: string[] = [];
    let values: any[][] = [];
    
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
    
    const rowValues: any[] = [];
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
      const nextRow: any[] = [];
      while (this.peek() !== ')') {
        nextRow.push(this.parseValue());
        if (this.peek() === ',') {
          this.consume();
        }
      }
      this.consume(')');
      values.push(nextRow);
    }
    
    if (this.peek() === ';') {
      this.consume();
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
      limit = parseInt(this.consume(), 10);
    }
    
    let offset: number | undefined;
    if (this.peek()?.toLowerCase() === 'offset') {
      this.consume();
      offset = parseInt(this.consume(), 10);
    }
    
    if (this.peek() === ';') {
      this.consume();
    }
    
    return { type: 'select', columns, tableName, where, orderBy, limit, offset };
  }

  private parseUpdate(): UpdateStatement {
    this.consume('update');
    const tableName = this.expectIdentifier();
    
    this.consume('set');
    
    const setClauses: { column: string; value: any }[] = [];
    
    do {
      const column = this.expectIdentifier();
      this.consume('=');
      const value = this.parseValue();
      setClauses.push({ column, value });
    } while (this.peek() === ',');
    
    let where: WhereClause | undefined;
    if (this.peek()?.toLowerCase() === 'where') {
      this.consume();
      where = this.parseWhereClause();
    }
    
    if (this.peek() === ';') {
      this.consume();
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
    
    if (this.peek() === ';') {
      this.consume();
    }
    
    return { type: 'delete', tableName, where };
  }

  private parseDropTable(): DropTableStatement {
    this.consume('drop');
    this.consume('table');
    const tableName = this.expectIdentifier();
    
    if (this.peek() === ';') {
      this.consume();
    }
    
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
    
    return this.parseComparisonClause();
  }

  private parseComparisonClause(): WhereClause {
    const left = this.parseExpression();

    const op = this.peek()?.toLowerCase();
    if (!op || !['=', '!=', '>', '>=', '<', '<=', 'like', 'in', 'between'].includes(op)) {
      throw new Error(`Expected comparison operator, got '${op}'`);
    }
    this.consume();

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
      operator: op as any,
      right
    };
  }
}

export function parse(sql: string): SqlStatement {
  const parser = new Parser(sql);
  return parser.parse();
}
