#!/usr/bin/env node
// Basic test script for YASD database

let YASD
try {
  YASD = require('../dist/index.js').YASD;
  console.log('Using compiled version from dist/index.js');
} catch {
  console.error('YASD build missing: run npm run build before running tests');
  process.exit(1);
}

function runTests() {
  console.log('Starting YASD tests...\n');

  let passed = 0;
  let failed = 0;

  function test(name, fn) {
    try {
      fn();
      console.log(`✓ ${name}`);
      passed++;
    } catch (error) {
      console.log(`✗ ${name}`);
      console.log(`  Error: ${error.message}`);
      failed++;
    }
  }

  function assert(condition, message) {
    if (!condition) {
      throw new Error(message || 'Assertion failed');
    }
  }

  // Test 1: Create database and table
  test('Create database and table', () => {
    const db = new YASD();
    const result = db.query('CREATE TABLE users (id int, name string, age number)');
    assert(result.affectedRows === 0, 'Should return 0 affected rows for CREATE TABLE');
    
    const tables = db.getTableNames();
    assert(tables.includes('users'), 'Table users should exist');
  });

  // Test 2: Insert data
  test('Insert data into table', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    
    const result = db.query("INSERT INTO users VALUES (1, 'John', 30)");
    assert(result.affectedRows === 1, 'Should insert 1 row');
    
    const result2 = db.query("INSERT INTO users VALUES (2, 'Jane', 25), (3, 'Bob', 35)");
    assert(result2.affectedRows === 2, 'Should insert 2 rows');
  });

  // Test 3: Select all data
  test('Select all data from table', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    db.query("INSERT INTO users VALUES (1, 'John', 30)");
    db.query("INSERT INTO users VALUES (2, 'Jane', 25)");
    
    const result = db.query('SELECT * FROM users');
    assert(result.columns.length === 3, 'Should return 3 columns');
    assert(result.rows.length === 2, 'Should return 2 rows');
    assert(result.rows[0].name === 'John', 'First row should be John');
    assert(result.rows[1].name === 'Jane', 'Second row should be Jane');
  });

  // Test 4: Select with WHERE clause
  test('Select with WHERE clause', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    db.query("INSERT INTO users VALUES (1, 'John', 30)");
    db.query("INSERT INTO users VALUES (2, 'Jane', 25)");
    db.query("INSERT INTO users VALUES (3, 'Bob', 35)");
    
    const result = db.query('SELECT * FROM users WHERE age > 25');
    assert(result.rows.length === 2, 'Should return 2 rows with age > 25');
    assert(result.rows.every(r => r.age > 25), 'All rows should have age > 25');
  });

  // Test 5: Update data
  test('Update data in table', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    db.query("INSERT INTO users VALUES (1, 'John', 30)");
    db.query("INSERT INTO users VALUES (2, 'Jane', 25)");
    
    const result = db.query('UPDATE users SET age = 31 WHERE id = 1');
    assert(result.affectedRows === 1, 'Should update 1 row');
    
    const check = db.query('SELECT * FROM users WHERE id = 1');
    assert(check.rows[0].age === 31, 'Age should be updated to 31');
  });

  // Test 6: Delete data
  test('Delete data from table', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    db.query("INSERT INTO users VALUES (1, 'John', 30)");
    db.query("INSERT INTO users VALUES (2, 'Jane', 25)");
    db.query("INSERT INTO users VALUES (3, 'Bob', 35)");
    
    const result = db.query('DELETE FROM users WHERE id = 2');
    assert(result.affectedRows === 1, 'Should delete 1 row');
    
    const check = db.query('SELECT * FROM users');
    assert(check.rows.length === 2, 'Should have 2 rows remaining');
    assert(!check.rows.some(r => r.id === 2), 'Row with id=2 should be deleted');
  });

  // Test 7: Drop table
  test('Drop table', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string)');
    
    const result = db.query('DROP TABLE users');
    assert(result.affectedRows === 0, 'Should return 0 affected rows for DROP TABLE');
    
    const tables = db.getTableNames();
    assert(!tables.includes('users'), 'Table users should not exist');
  });

  // Test 8: Select with ORDER BY
  test('Select with ORDER BY', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    db.query("INSERT INTO users VALUES (1, 'John', 30)");
    db.query("INSERT INTO users VALUES (2, 'Jane', 25)");
    db.query("INSERT INTO users VALUES (3, 'Bob', 35)");
    
    const result = db.query('SELECT * FROM users ORDER BY age DESC');
    assert(result.rows.length === 3, 'Should return all rows');
    assert(result.rows[0].age === 35, 'First row should have highest age');
    assert(result.rows[1].age === 30, 'Second row should have middle age');
    assert(result.rows[2].age === 25, 'Third row should have lowest age');
  });

  // Test 9: Select with LIMIT and OFFSET
  test('Select with LIMIT and OFFSET', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string)');
    db.query("INSERT INTO users VALUES (1, 'John')");
    db.query("INSERT INTO users VALUES (2, 'Jane')");
    db.query("INSERT INTO users VALUES (3, 'Bob')");
    db.query("INSERT INTO users VALUES (4, 'Alice')");
    
    const result = db.query('SELECT * FROM users LIMIT 2 OFFSET 1');
    assert(result.rows.length === 2, 'Should return 2 rows');
    assert(result.rows[0].id === 2, 'First row should be id=2');
    assert(result.rows[1].id === 3, 'Second row should be id=3');
  });

  // Test 10: Table schema
  test('Get table schema', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string, age number)');
    
    const schema = db.getTableSchema('users');
    assert(schema !== undefined, 'Schema should exist');
    assert(schema.name === 'users', 'Schema name should be users');
    assert(schema.columns.length === 3, 'Should have 3 columns');
    assert(schema.columns[0].name === 'id', 'First column should be id');
    assert(schema.columns[1].name === 'name', 'Second column should be name');
    assert(schema.columns[2].name === 'age', 'Third column should be age');
  });

  // Test 11: Reset database
  test('Reset database', () => {
    const db = new YASD();
    db.query('CREATE TABLE users (id int, name string)');
    db.query("INSERT INTO users VALUES (1, 'John')");
    
    assert(db.getTableNames().includes('users'), 'Table should exist before reset');
    
    db.reset();
    
    assert(db.getTableNames().length === 0, 'No tables should exist after reset');
  });

  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(`Tests completed: ${passed + failed}`);
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  console.log('='.repeat(50));

  if (failed > 0) {
    process.exit(1);
  }
}

// Run tests
runTests();
