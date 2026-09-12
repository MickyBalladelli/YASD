const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const root = path.resolve(__dirname, "..");
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "yasd-package-"));
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
function run(command, args, cwd = dir) {
  return execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    timeout: 120_000,
  });
}
try {
  run(npm, ["pack", "--pack-destination", dir, "--json"], root);
  const tarballs = fs.readdirSync(dir).filter((name) => name.endsWith(".tgz"));
  assert.equal(tarballs.length, 1);
  const files = [{ filename: tarballs[0] }];
  fs.writeFileSync(
    path.join(dir, "package.json"),
    '{"name":"yasd-consumer","private":true}',
  );
  run(npm, [
    "install",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    path.join(dir, files[0].filename),
  ]);
  fs.writeFileSync(
    path.join(dir, "consumer.cjs"),
    `
    const assert = require('node:assert/strict');
    const { YASD, YasdServer, DatabaseError, DATABASE_ERROR_CODES } = require('yasd');
    const metadata = require('yasd/package.json');
    assert.equal(typeof require('yasd/examples/production-integration').ProductionPostCache, 'function');
    const db = new YASD({ sweepIntervalMs: 0 });
    db.query('CREATE TABLE sample (id int primary key)');
    db.query('INSERT INTO sample VALUES (1)');
    assert.equal(db.query('SELECT * FROM sample').rows[0].id, 1);
    assert.throws(() => db.query('SELECT * FROM missing'), e => e instanceof DatabaseError && e.code === DATABASE_ERROR_CODES.TABLE_NOT_FOUND);
    db.close();
    const server = new YasdServer();
    assert.equal(server.info().version, metadata.version);
    server.close().catch(e => { throw e; });
  `,
  );
  run(process.execPath, ["consumer.cjs"]);
  const readme = fs.readFileSync(
    path.join(dir, "node_modules/yasd/README.md"),
    "utf8",
  );
  const blocks = [...readme.matchAll(/```javascript\n([\s\S]*?)```/g)]
    .slice(0, 3)
    .map((match) => match[1]);
  assert.equal(blocks.length, 3);
  fs.writeFileSync(path.join(dir, "readme.cjs"), blocks.join("\n"));
  run(process.execPath, ["readme.cjs"]);
  fs.writeFileSync(
    path.join(dir, "consumer.mjs"),
    `import { YASD, DatabaseError } from 'yasd'; const db = new YASD(); if (!DatabaseError) throw Error('missing export'); db.close();`,
  );
  run(process.execPath, ["consumer.mjs"]);
  fs.writeFileSync(
    path.join(dir, "consumer.ts"),
    `import { YASD, DatabaseError, DatabaseErrorCode } from 'yasd'; const db = new YASD({slowQueryMs: 5}); const code: DatabaseErrorCode = 'PARSE_ERROR'; const error = new DatabaseError('message', code); db.close();`,
  );
  run(process.execPath, [
    path.join(root, "node_modules/typescript/bin/tsc"),
    "--noEmit",
    "--strict",
    "--target",
    "ES2020",
    "--module",
    "node16",
    "--moduleResolution",
    "node16",
    "--typeRoots",
    path.join(root, "node_modules/@types"),
    "consumer.ts",
  ]);
  assert.match(
    run(process.execPath, ["node_modules/yasd/dist/cli.js", "--help"]),
    /tls-min-version/,
  );
  const stage = path.join(dir, "dependency-stage");
  fs.mkdirSync(stage);
  for (const name of ["package.json", "package-lock.json"])
    fs.copyFileSync(path.join(root, name), path.join(stage, name));
  run(npm, ["ci", "--offline", "--no-audit", "--no-fund"], stage);
  assert.equal(fs.existsSync(path.join(stage, "dist")), false);
  console.log(
    "Installed package: CommonJS, ESM, declarations, version, CLI and package-only npm ci passed.",
  );
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
