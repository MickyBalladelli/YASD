#!/usr/bin/env node
// Test server for YASD database package

const http = require('http');
const url = require('url');

// Import YASD - this will use the compiled version
// First, let's try to use the source directly for testing
let YASD;
try {
  // Try to use the compiled version first
  YASD = require('../dist/index.js').YASD;
} catch (e) {
  console.log('Using source version (run npm run build first for production)');
  // For development, we might need to compile first
  // For now, we'll create a simple version
}

// Create a new database instance
let db;

function initDatabase() {
  if (YASD) {
    db = new YASD();
  } else {
    // Fallback implementation for testing without compilation
    db = createSimpleYASD();
  }

  // Initialize with some test data
  try {
    db.query(`
      CREATE TABLE users (
        id int primary key,
        name string,
        email string,
        age number
      )
    `);

    db.query(`
      CREATE TABLE products (
        id int primary key,
        name string,
        price number,
        in_stock boolean
      )
    `);

    // Insert some test data
    db.query("INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', 30)");
    db.query("INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com', 25)");
    db.query("INSERT INTO users VALUES (3, 'Bob Johnson', 'bob@example.com', 35)");

    db.query("INSERT INTO products VALUES (1, 'Laptop', 999.99, true)");
    db.query("INSERT INTO products VALUES (2, 'Phone', 699.99, true)");
    db.query("INSERT INTO products VALUES (3, 'Tablet', 399.99, false)");

    console.log('Database initialized with test data');
  } catch (error) {
    console.error('Error initializing database:', error.message);
  }
}

// Simple fallback implementation for testing without compilation
function createSimpleYASD() {
  const tables = new Map();

  return {
    query: function(sql) {
      sql = sql.trim().toLowerCase();
      
      // Very simple query handling for testing
      if (sql.startsWith('create table')) {
        const match = sql.match(/create table (\w+)/);
        if (match) {
          tables.set(match[1], { rows: [] });
          return { columns: [], rows: [], affectedRows: 0 };
        }
      } else if (sql.startsWith('insert into')) {
        const match = sql.match(/insert into (\w+)/);
        if (match && tables.has(match[1])) {
          const table = tables.get(match[1]);
          table.rows.push({});
          return { columns: [], rows: [], affectedRows: 1 };
        }
      } else if (sql.startsWith('select')) {
        const match = sql.match(/select \* from (\w+)/);
        if (match && tables.has(match[1])) {
          const table = tables.get(match[1]);
          return { 
            columns: ['data'],
            rows: table.rows.map(r => ({...r}))
          };
        }
      }
      
      return { columns: [], rows: [], affectedRows: 0 };
    },
    
    getTableNames: function() {
      return Array.from(tables.keys());
    },
    
    getTableSchema: function() {
      return undefined;
    },
    
    reset: function() {
      tables.clear();
    }
  };
}

// Initialize database
initDatabase();

// Create HTTP server
const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const path = parsedUrl.pathname;
  const method = req.method.toUpperCase();

  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Request-Method', '*');
  res.setHeader('Access-Control-Allow-Methods', 'OPTIONS, GET, POST');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // Handle different endpoints
  if (path === '/') {
    handleHome(res);
  } else if (path === '/api/tables') {
    handleGetTables(req, res);
  } else if (path.startsWith('/api/query')) {
    handleQuery(req, res);
  } else if (path === '/api/reset') {
    handleReset(req, res);
  } else {
    handleNotFound(res);
  }
});

function handleHome(res) {
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>YASD Database Test Server</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }
        .container {
            display: flex;
            gap: 20px;
        }
        .panel {
            flex: 1;
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        textarea {
            width: 100%;
            height: 150px;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 14px;
        }
        button {
            background: #007bff;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
            margin-top: 10px;
        }
        button:hover {
            background: #0056b3;
        }
        pre {
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
            font-family: 'Monaco', 'Menlo', monospace;
        }
        .result {
            margin-top: 20px;
        }
        .status {
            padding: 10px;
            border-radius: 4px;
            margin: 10px 0;
        }
        .success {
            background: #d4edda;
            color: #155724;
        }
        .error {
            background: #f8d7da;
            color: #721c24;
        }
        .tables {
            list-style: none;
            padding: 0;
        }
        .tables li {
            padding: 8px;
            border-bottom: 1px solid #eee;
        }
        .tables li:last-child {
            border-bottom: none;
        }
        .sql-examples {
            margin-top: 20px;
        }
        .sql-examples h3 {
            margin-bottom: 10px;
            font-size: 16px;
        }
        .sql-examples div {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 10px;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 13px;
            cursor: pointer;
        }
        .sql-examples div:hover {
            background: #e9ecef;
        }
    </style>
</head>
<body>
    <h1>YASD Database Test Server</h1>
    
    <div class="container">
        <div class="panel">
            <h2>SQL Query</h2>
            <textarea id="sqlQuery" placeholder="Enter SQL query...">SELECT * FROM users</textarea>
            <button onclick="executeQuery()">Execute</button>
            
            <div class="sql-examples">
                <h3>Example Queries</h3>
                <div onclick="setQuery('SELECT * FROM users')">SELECT * FROM users</div>
                <div onclick="setQuery('SELECT * FROM products')">SELECT * FROM products</div>
                <div onclick="setQuery('SELECT * FROM users WHERE age > 25')">SELECT * FROM users WHERE age > 25</div>
                <div onclick="setQuery('INSERT INTO users VALUES (4, \\'Alice\\', \\'alice@example.com\\', 28)')">INSERT INTO users VALUES (4, 'Alice', 'alice@example.com', 28)</div>
                <div onclick="setQuery('UPDATE users SET age = 31 WHERE id = 1')">UPDATE users SET age = 31 WHERE id = 1</div>
                <div onclick="setQuery('DELETE FROM users WHERE id = 4')">DELETE FROM users WHERE id = 4</div>
            </div>
        </div>
        
        <div class="panel">
            <h2>Results</h2>
            <div id="result" class="result"></div>
            
            <h2>Tables</h2>
            <div id="tables" class="tables"></div>
            
            <button onclick="refreshTables()" style="margin-top: 10px;">Refresh Tables</button>
            <button onclick="resetDatabase()" style="margin-top: 10px; background: #dc3545;">Reset Database</button>
        </div>
    </div>

    <script>
        function executeQuery() {
            const query = document.getElementById('sqlQuery').value;
            
            fetch('/api/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ sql: query })
            })
            .then(response => response.json())
            .then(data => {
                displayResult(query, data);
                refreshTables();
            })
            .catch(error => {
                displayResult(query, { error: error.message });
            });
        }

        function displayResult(query, data) {
            const resultDiv = document.getElementById('result');
            let html = '<h3>Query:</h3><pre>' + escapeHtml(query) + '</pre>';
            
            if (data.error) {
                html += '<div class="status error">Error: ' + escapeHtml(data.error) + '</div>';
            } else {
                html += '<div class="status success">Success</div>';
                
                if (data.rows && data.rows.length > 0) {
                    html += '<h3>Results (' + data.rows.length + ' rows)</h3>';
                    html += '<table border="1" cellpadding="5" style="border-collapse: collapse; width: 100%;">';
                    html += '<thead><tr>';
                    data.columns.forEach(col => {
                        html += '<th style="background: #f8f9fa; text-align: left;">' + escapeHtml(col) + '</th>';
                    });
                    html += '</tr></thead>';
                    html += '<tbody>';
                    data.rows.forEach(row => {
                        html += '<tr>';
                        data.columns.forEach(col => {
                            html += '<td style="text-align: left;">' + escapeHtml(String(row[col] || 'NULL')) + '</td>';
                        });
                        html += '</tr>';
                    });
                    html += '</tbody></table>';
                } else if (data.affectedRows !== undefined) {
                    html += '<div class="status success">Affected rows: ' + data.affectedRows + '</div>';
                } else if (data.rows && data.rows.length === 0) {
                    html += '<div class="status success">No rows returned</div>';
                }
            }
            
            resultDiv.innerHTML = html;
        }

        function setQuery(sql) {
            document.getElementById('sqlQuery').value = sql;
        }

        function refreshTables() {
            fetch('/api/tables')
            .then(response => response.json())
            .then(data => {
                const tablesDiv = document.getElementById('tables');
                let html = '';
                
                if (data.tables && data.tables.length > 0) {
                    data.tables.forEach(table => {
                        html += '<li><strong>' + escapeHtml(table.name) + '</strong> (' + table.rowCount + ' rows)</li>';
                    });
                } else {
                    html = '<li>No tables</li>';
                }
                
                tablesDiv.innerHTML = html;
            })
            .catch(error => {
                console.error('Error fetching tables:', error);
            });
        }

        function resetDatabase() {
            if (confirm('Are you sure you want to reset the database? All data will be lost.')) {
                fetch('/api/reset', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    displayResult('RESET DATABASE', data);
                    refreshTables();
                })
                .catch(error => {
                    console.error('Error resetting database:', error);
                });
            }
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        // Initialize
        refreshTables();
    </script>
</body>
</html>
  `);
}

function handleGetTables(req, res) {
  if (req.method !== 'GET') {
    res.writeHead(405, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  try {
    const tableNames = db.getTableNames();
    const tables = tableNames.map(name => {
      try {
        const result = db.query(`SELECT COUNT(*) as count FROM ${name}`);
        const rowCount = result.rows.length > 0 ? result.rows[0].count || result.rows.length : 0;
        return { name, rowCount };
      } catch (e) {
        // Fallback to rows length
        const table = db.getTableSchema(name);
        return { name, rowCount: 0 };
      }
    });

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ tables }));
  } catch (error) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: error.message }));
  }
}

function handleQuery(req, res) {
  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  let body = '';
  req.on('data', chunk => {
    body += chunk.toString();
  });

  req.on('end', () => {
    try {
      const { sql } = JSON.parse(body);
      
      if (!sql) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'SQL query is required' }));
        return;
      }

      const result = db.query(sql);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
    }
  });
}

function handleReset(req, res) {
  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  try {
    db.reset();
    // Re-initialize with test data
    initDatabase();
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
      success: true, 
      message: 'Database reset and re-initialized with test data' 
    }));
  } catch (error) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: error.message }));
  }
}

function handleNotFound(res) {
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
}

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`YASD Test Server running on http://localhost:${PORT}`);
  console.log('Press Ctrl+C to stop the server');
});

module.exports = server;
