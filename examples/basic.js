#!/usr/bin/env node
// Basic example of using YASD

const { YASD } = require('../dist/index.js');

// Create a new database
const db = new YASD();

console.log('=== YASD Basic Example ===\n');

// Create a table
console.log('1. Creating table...');
db.query(`
  CREATE TABLE users (
    id int primary key,
    name string,
    email string,
    age number,
    active boolean
  )
`);
console.log('   Table "users" created\n');

// Insert data
console.log('2. Inserting data...');
db.query("INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', 30, true)");
db.query("INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com', 25, false)");
db.query("INSERT INTO users VALUES (3, 'Bob Johnson', 'bob@example.com', 35, true)");
console.log('   3 users inserted\n');

// Query all data
console.log('3. Querying all users:');
const allUsers = db.query('SELECT * FROM users');
console.log('   Columns:', allUsers.columns.join(', '));
allUsers.rows.forEach(row => {
  console.log('   -', row.name, '(', row.age, ')', '-', row.email, '- Active:', row.active);
});
console.log();

// Query with WHERE
console.log('4. Querying active users:');
const activeUsers = db.query('SELECT name, age FROM users WHERE active = true');
activeUsers.rows.forEach(row => {
  console.log('   -', row.name, 'is', row.age, 'years old');
});
console.log();

// Query with ORDER BY
console.log('5. Querying users by age (descending):');
const orderedUsers = db.query('SELECT name, age FROM users ORDER BY age DESC');
orderedUsers.rows.forEach(row => {
  console.log('   -', row.name, ':', row.age);
});
console.log();

// Update data
console.log('6. Updating John\'s age...');
db.query('UPDATE users SET age = 31 WHERE name = \'John Doe\'');
const updatedJohn = db.query('SELECT name, age FROM users WHERE name = \'John Doe\'');
console.log('   John is now', updatedJohn.rows[0].age, 'years old\n');

// Insert with specific columns
console.log('7. Inserting with specific columns:');
db.query("INSERT INTO users (name, email, age) VALUES ('Alice Brown', 'alice@example.com', 28)");
const alice = db.query('SELECT * FROM users WHERE name = \'Alice Brown\'');
console.log('   Alice:', alice.rows[0]);
console.log();

// Delete data
console.log('8. Deleting inactive users...');
const deleted = db.query('DELETE FROM users WHERE active = false');
console.log('   Deleted', deleted.affectedRows, 'users\n');

// Count remaining users
console.log('9. Counting remaining users:');
const remaining = db.query('SELECT COUNT(*) as count FROM users');
console.log('   Remaining users:', remaining.rows[0].count, '\n');

// Table information
console.log('10. Database info:');
console.log('    Tables:', db.getTableNames().join(', '));
console.log('    Schema for users:', JSON.stringify(db.getTableSchema('users').columns, null, 2));

// Reset database
console.log('\n11. Resetting database...');
db.reset();
console.log('    Database reset. Tables:', db.getTableNames().length);

console.log('\n=== Example Complete ===');
