// Run every suite even when an earlier suite fails; never use stale build output.
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '..');
let failures = 0;
for (const name of fs.readdirSync(__dirname).filter(name => name.endsWith('.test.js')).sort()) {
  console.log(`\n--- ${name} ---`);
  const result = spawnSync(process.execPath, [path.join(__dirname, name)], {
    cwd: root, stdio: 'inherit', timeout: 120_000,
  });
  if (result.error || result.status !== 0) { failures++; console.error(`${name} failed`, result.error || result.status); }
}
console.log(`\nSuite failures: ${failures}`);
process.exitCode = failures ? 1 : 0;
